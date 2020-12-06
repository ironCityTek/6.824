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

	sockname string

	done bool
}

// Shutdown is a method that shuts down the Master's RPC server.
func (m *Master) Shutdown() error {
	go func() { m.l.Close() }()
	return nil
}

func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
	m.Lock()
	fmt.Printf("registering %s\n", args.Address)
	m.availableWorkers = append(m.availableWorkers, args.Address)
	m.Unlock()
	reply.Success = true

	m.cond.Broadcast()

	return nil
}

func (m *Master) scheduleWork(jobName string) {
	var wg sync.WaitGroup

	switch jobName {
	case "map":
		for i := 0; i < m.nMap; i++ {
			wg.Add(1)

			go func() {
				success := false

				m.Lock()
				var jobNo int
				m.mapCalls++
				jobNo = m.mapCalls
				m.Unlock()
				for !success {
					worker := <-m.workChannel

					m.Lock()
					var filename string
					filename, m.files = m.files[len(m.files)-1], m.files[:len(m.files)-1]
					fmt.Println(worker, filename)
					m.Unlock()

					var args = StartWorkArgs{JobName: jobName, Filename: filename, JobNo: jobNo, NReduce: m.nReduce}
					var reply = StartWorkReply{}

					success = call(worker, "Workr.StartWork", &args, &reply)

					if success {
						go func() {
							m.workChannel <- worker
							wg.Done()
						}()
					} else {
						fmt.Printf("failed %q %q\n", worker, filename)
						// if !success put file back on m.files
						m.Lock()
						m.files = append(m.files, filename)
						m.Unlock()
					}
				}
			}()
		}

	case "reduce":
		for i := 0; i < m.nReduce; i++ {
			wg.Add(1)

			bucketNumber := i

			go func() {
				success := false

				for !success {
					fmt.Println("try? try again?")
					worker := <-m.workChannel
					fmt.Printf("made it, %s\n", worker)
					var args = StartWorkArgs{JobName: jobName, JobNo: m.mapCalls, NReduce: bucketNumber}
					var reply = StartWorkReply{}

					success = call(worker, "Workr.StartWork", &args, &reply)
					fmt.Printf("%s success? %t\n", worker, success)

					if success {
						go func() {
							fmt.Printf("putting %s back in workchannel\n", worker)
							m.workChannel <- worker
							wg.Done()
						}()
					} else {
						fmt.Printf("failed reduce %s\n", worker)
					}
				}
			}()
		}

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

					fmt.Printf("putting worker in workchannel %s\n", availableWorker)
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
	m.sockname = masterSock()
	os.Remove(m.sockname)
	l, e := net.Listen("unix", m.sockname)
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
	close(m.workChannel)
	fmt.Printf("ending -- %d in queue, %d in workchannel\n", len(m.availableWorkers), len(m.workChannel))
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
		} else {
			// clean up /var/tmp
			e := os.Remove(worker)
			if e != nil {
				log.Fatal(e)
			}
		}
	}

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
	workChannel := make(chan string, 16)
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
