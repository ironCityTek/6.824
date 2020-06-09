package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Master struct {
	// Your definitions here.
	sync.Mutex

	mapCalls int

	location string

	newCond          *sync.Cond
	availableWorkers []string

	files []string

	nReduce int

	doneChannel chan bool
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
	m.availableWorkers = append(m.availableWorkers, args.Address)

	fmt.Println(m.availableWorkers)
	reply.Success = true
	// worker is ready to receive work
	// give worker job to do
	// move job to pending map
	// when job finishes remove from pending map

	m.workAssignment()
	return nil
}

func (m *Master) workAssignment() {
	availableWorker := m.availableWorkers[0]

	var filename string
	filename, m.files = m.files[len(m.files)-1], m.files[:len(m.files)-1]

	m.mapCalls++
	var args = StartWorkArgs{JobName: "mapJob", Filename: filename, JobNo: m.mapCalls, NReduce: m.nReduce}

	var reply = StartWorkReply{}

	call(availableWorker, "Workr.StartWork", &args, &reply)

	fmt.Println(len(m.files))
	if len(m.files) == 0 {
		fmt.Println("Done with mapping")
	}
}

// func schedule(worker string) {

// }

// func (m *Master) manageRegistrations(ch chan string) {
// 	i := 0
// 	for {
// 		mr.Lock()

// 	}
// }

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
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

	m.server()
	return &m
}
