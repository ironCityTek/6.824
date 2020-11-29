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

	nMap    int
	nReduce int

	l net.Listener

	shutdownChan chan bool

	done bool
}

// Shutdown is an RPC method that shuts down the Master's RPC server.
func (m *Master) Shutdown(_, _ *struct{}) error {
	close(m.shutdownChan)
	m.l.Close()
	return nil
}

func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
	m.availableWorkers = append(m.availableWorkers, args.Address)
	reply.Success = true

	m.cond.Broadcast()

	return nil
}

func (m *Master) scheduleWork(jobName string) {
	workChannel := make(chan string)
	// better variable names?
	var tasks int
	// var other int

	switch jobName {
	case "map":
		tasks = m.nMap
		// other = m.nReduce
	case "reduce":
		tasks = m.nMap
		// other = m.nReduce
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", tasks, jobName, tasks)

	var wg sync.WaitGroup

	for i := 0; i < tasks; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			success := false

			for !success {
				worker := <-workChannel
				m.Lock()
				var filename string
				filename, m.files = m.files[len(m.files)-1], m.files[:len(m.files)-1]

				m.mapCalls++
				// runtime.Breakpoint()
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
					// safety clause in case there's only 1 available worker
					if popIndex < 0 {
						popIndex = 0
					}
					availableWorker, m.availableWorkers = m.availableWorkers[popIndex], m.availableWorkers[:popIndex]

					workChannel <- availableWorker
				} else {
					m.cond.Wait() // wait for a new worker to be registered
					// m.Lock()
				}
				m.Unlock()
			}
		}
	}()
	wg.Wait()

}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpcs := rpc.NewServer()
	rpcs.Register(m)
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	fmt.Println(sockname)
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
	return m.done
}

func (m *Master) end() {
	// send shutdown messages to all workers,
	// stop RPC server
	// send done message
	m.done = true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// TODO: find a better way to derive "done" state
	m := Master{files: files, nMap: len(files), nReduce: nReduce, done: false}
	m.mapCalls = -1
	m.cond = sync.NewCond(&m)

	m.server()
	m.scheduleWork("map")
	// m.scheduleWork("reduce")
	// m.end()
	return &m
}
