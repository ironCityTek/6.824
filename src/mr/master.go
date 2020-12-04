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
	workChannel      chan string

	files []string

	nMap    int
	nReduce int

	l net.Listener

	done bool
}

// Shutdown is an RPC method that shuts down the Master's RPC server.
func (m *Master) Shutdown() error {
	go func() { m.l.Close() }()
	return nil
}

func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
	m.availableWorkers = append(m.availableWorkers, args.Address)
	reply.Success = true

	m.cond.Broadcast()

	return nil
}

func (m *Master) scheduleWork(jobName string) {
	// better variable names?
	var tasks int
	// var other int

	switch jobName {
	case "map":
		tasks = m.nMap
		// other = m.nReduce
	case "reduce":
		tasks = m.nReduce
		// other = m.nReduce
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", tasks, jobName, tasks)

	var wg sync.WaitGroup

	for i := 0; i < tasks; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			success := false

			m.Lock()
			var jobNo int
			switch jobName {
			case "map":
				m.mapCalls++
				jobNo = m.mapCalls
			case "reduce":
				jobNo = m.mapCalls
			}
			m.Unlock()
			for !success {
				worker := <-m.workChannel
				m.Lock()
				var filename string
				if jobName == "map" {
					filename, m.files = m.files[len(m.files)-1], m.files[:len(m.files)-1]
				}

				m.Unlock()
				var args = StartWorkArgs{JobName: jobName, Filename: filename, JobNo: jobNo, NReduce: m.nReduce}
				var reply = StartWorkReply{}

				fmt.Printf("calling %s\n", worker)
				success = call(worker, "Workr.StartWork", &args, &reply)

				if success {
					go func() { m.workChannel <- worker }()
				} else {
					// if !success put file back on m.files
					m.Lock()
					m.files = append(m.files, filename)
					m.Unlock()
				}
				fmt.Printf("job No #%d \n", jobNo)
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
					// do I need this clause?
					if popIndex < 0 {
						popIndex = 0
					}
					availableWorker, m.availableWorkers = m.availableWorkers[popIndex], m.availableWorkers[:popIndex]

					m.workChannel <- availableWorker
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
		for {
			// select {
			// case <-m.shutdownChan:
			// 	break loop
			// default:
			// }
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
	if m.done == true {
		go func() {
			// stop RPC server
			m.Shutdown()
		}()
	}
	return m.done
}

func (m *Master) end() {
	// send shutdown messages to all workers
	for len(m.availableWorkers) > 0 {
		var worker string
		m.Lock()
		popIndex := len(m.availableWorkers) - 1
		// if popIndex < 0 {
		// 	popIndex = 0
		// }
		worker, m.availableWorkers = m.availableWorkers[popIndex], m.availableWorkers[:popIndex]
		m.Unlock()

		var reply ShutdownReply
		success := call(worker, "Worker.Shutdown", new(struct{}), &reply)
		if success == false {
			fmt.Printf("Cleanup: RPC %s error\n", worker)
			// if !success put file back on m.files
			m.Lock()
			m.availableWorkers = append(m.availableWorkers, worker)
			m.Unlock()
		}
	}

	// close(m.workChannel)

	for worker := range m.workChannel {
		var reply ShutdownReply
		success := call(worker, "Workr.Shutdown", new(struct{}), &reply)
		if success == false {
			fmt.Printf("Cleanup: RPC %s error\n", worker)
		}
	}

	// set done message
	m.done = true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	workChannel := make(chan string)
	// TODO: find a better way to derive "done" state
	m := Master{files: files, nMap: len(files), nReduce: nReduce, done: false, workChannel: workChannel}
	m.mapCalls = -1
	m.cond = sync.NewCond(&m)

	m.server()
	m.scheduleWork("map")
	m.scheduleWork("reduce")
	m.end()
	return &m
}
