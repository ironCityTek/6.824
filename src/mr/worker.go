package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type Workr struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string

	l net.Listener
}

// TODO
// func (w *Workr) Shutdown(_ *struct{}, res *ShutdownReply) error {
// 	w.Lock()
// 	defer w.Unlock()

// }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (w *Workr) StartWork(args *StartWorkArgs, _ *struct{}) error {
	// runtime.Breakpoint()
	var err error
	fmt.Println(args)
	switch args.JobName {
	case "map":
		var file *os.File
		if file, err = os.Open(args.Filename); err != nil {
			log.Fatalf("cannot load file %v", args.Filename)
		}

		var content []byte
		if content, err = ioutil.ReadAll(file); err != nil {
			log.Fatalf("cannot open file %v", args.Filename)
		}

		kva := w.mapf(args.Filename, string(content))

		tempFiles := make(map[string]*os.File)
		for i := 0; i < args.NReduce; i++ {
			fmt.Println(fmt.Sprint(i))
			name := fmt.Sprintf("/var/tmp/mr-%d-%d", args.JobNo, i)
			f, _ := os.Create(name)
			tempFiles[name] = f
			defer f.Close()
		}

		fmt.Println(tempFiles)

		for _, kv := range kva {
			reduceBucket := ihash(kv.Key) % 10
			name := fmt.Sprintf("/var/tmp/mr-%d-%d", args.JobNo, reduceBucket)
			writer := bufio.NewWriter(tempFiles[name])
			enc := json.NewEncoder(writer)
			enc.Encode(&kv)
			writer.Flush()
		}

	}

	return nil
}

// function to write map output to temporary local file
// func emit() {

// }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	w := new(Workr)

	w.mapf = mapf
	w.reducef = reducef

	rpcs := rpc.NewServer()
	rpcs.Register(w)

	sockname := "/var/tmp/824-mr-" + strconv.Itoa(os.Getpid())
	fmt.Println("@@@" + sockname)
	os.Remove(sockname)

	l, e := net.Listen("unix", sockname)
	fmt.Println("!!!!!")

	if e != nil {
		log.Fatal("listen error:", e)
	}
	w.l = l
	// w.register()

	// give the worker time to launch
	time.Sleep(time.Second / 1000)

	args := RegisterArgs{Address: sockname}

	reply := RegisterReply{}

	call(masterSock(), "Master.Register", &args, &reply)

	for {
		conn, err := w.l.Accept()
		if err == nil {
			// w.Lock()
			// w.nRPC--
			// w.Unlock()
			go rpcs.ServeConn(conn)
		} else {
			break
		}
	}

	// socketname := masterSock()
	// Your worker implementation here.
	// client, err := rpc.DialHTTP("unix", socketname)
	// if err != nil {
	// 	log.Fatal("dailing:", err)
	// }

	//////////////

	// call("Master.RetrieveFile", &args, &reply)
	// // call("Master.Ready", &args, &reply)

	// fmt.Println(reply)
	// file, err := os.Open(reply.Filename)
	// if err != nil {
	// 	log.Fatalf("cannot open %v", reply.Filename)
	// }

	// content, err := ioutil.ReadAll(file)
	// if err != nil {
	// 	log.Fatalf("cannot open %v", reply.Filename)
	// }

	// file.Close()

	// kva := mapf(reply.Filename, string(content))

	// oname := "mr-out-x"
	// ofile, _ := os.Create(oname)

	// fmt.Println(len(kva))

	// uncomment to send the Example RPC to the master.
	// CallExample()

}
