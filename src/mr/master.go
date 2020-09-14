package mr

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
)

type Master struct {
	// Your definitions here.
	sync.Mutex

	cond *sync.Cond

	mapCalls int

	availableWorkers []string

	files []string

	nReduce int

	doneChannel chan bool

	l net.Listener

	shutdownChan chan bool
}

// Shutdown is an RPC method that shuts down the Master's RPC server.
func (m *Master) Shutdown(_, _ *struct{}) error {
	close(m.shutdownChan)
	m.l.Close()
	return nil
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// func (m *Master) RetrieveFile(args *RetrieveFile, reply *RetrieveFileReply) error {
// 	reply.Filename, m.files = m.files[len(m.files)-1], m.files[:len(m.files)-1]
// 	return nil
// }

func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
	// runtime.Breakpoint()
	m.availableWorkers = append(m.availableWorkers, args.Address)

	fmt.Println(m.availableWorkers)
	reply.Success = true

	m.cond.Broadcast()

	return nil
}

func (m *Master) scheduleWork(jobName string) {
	workChannel := make(chan string)

	tasks := len(m.files)

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", tasks, jobName, m.nReduce)

	var wg sync.WaitGroup

	for i := 0; i < tasks; i++ {
		fmt.Println(i)
		wg.Add(1)

		go func() {
			defer wg.Done()
			success := false

			for !success {
				fmt.Println("b4")
				worker := <-workChannel
				fmt.Println("after")
				m.Lock()
				var filename string
				filename, m.files = m.files[len(m.files)-1], m.files[:len(m.files)-1]

				m.mapCalls++
				m.Unlock()
				var args = StartWorkArgs{JobName: jobName, Filename: filename, JobNo: m.mapCalls, NReduce: m.nReduce}
				var reply = StartWorkReply{}

				fmt.Printf("calling %s\n", worker)
				success = call(worker, "Workr.StartWork", &args, &reply)

				if success {
					go func() { workChannel <- worker }()
				} else {
					// if !success put file back on m.files
					m.Lock()
					m.files = append(m.files, filename)
					m.Unlock()
				}
			}
		}()
	}

	/*
		use channel to signal while loop to run again when
		worker finishes or is added to available workers

		waitgroup to wait for all the map jobs are finished, then proceed to reduce phase
	*/
	go func() {
		for {
			m.Lock()
			if len(m.files) > 0 {
				if len(m.availableWorkers) > 0 {
					var availableWorker string
					popIndex := len(m.availableWorkers) - 1
					if popIndex < 0 {
						popIndex = 0
					}
					availableWorker, m.availableWorkers = m.availableWorkers[popIndex], m.availableWorkers[:popIndex]

					workChannel <- availableWorker
				} else {
					m.cond.Wait() // wait for a new worker to be registered
				}
				m.Unlock()
			}
		}
	}()
	wg.Wait()

	fmt.Println("done mapping")
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpcs := rpc.NewServer()
	rpcs.Register(m)
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	m.l = l

	go func() {
	loop:
		for {
			select {
			case <-m.shutdownChan:
				break loop
			default:
			}
			conn, err := m.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				break
			}
		}
	}()
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// runtime.Breakpoint()
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{files: files, nReduce: nReduce}
	m.mapCalls = -1
	m.cond = sync.NewCond(&m)

	m.server()
	m.scheduleWork("map")
	// m.scheduleWork("reduce")
	// m.end()
	return &m
}
