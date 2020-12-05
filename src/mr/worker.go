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

func (w *Workr) Shutdown(_ *struct{}, res *ShutdownReply) error {
	go func() { w.l.Close() }()

	return nil
}

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
	var err error
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
			name := fmt.Sprintf("/var/tmp/mr-%d-%d", args.JobNo, i)
			f, _ := os.Create(name)
			tempFiles[name] = f
			defer f.Close()
		}

		for _, kv := range kva {
			reduceBucket := ihash(kv.Key) % args.NReduce
			name := fmt.Sprintf("/var/tmp/mr-%d-%d", args.JobNo, reduceBucket)
			writer := bufio.NewWriter(tempFiles[name])
			enc := json.NewEncoder(writer)
			enc.Encode(&kv)
			writer.Flush()
		}

	case "reduce":
		kvMap := make(map[string][]string)
		for mapCall := 0; mapCall < args.JobNo; mapCall++ {
			var file *os.File
			filename := fmt.Sprintf("/var/tmp/mr-%d-%d", mapCall, args.NReduce)
			if file, err = os.Open(filename); err != nil {
				log.Fatalf("cannot load file %v", filename)
			}
			defer file.Close()

			// need file to be of type "Reader"
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
			}
		}

		outfileName := fmt.Sprintf("mr-out-%d", args.NReduce)

		var outfile *os.File
		if outfile, err = os.Create(outfileName); err != nil {
			log.Fatalf("failed to create file %v", outfileName)
		}
		defer outfile.Close()

		enc := json.NewEncoder(outfile)

		for key, values := range kvMap {
			result := w.reducef(key, values)
			enc.Encode(&KeyValue{key, result})
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

	if e != nil {
		log.Fatal("listen error:", e)
	}
	w.l = l

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

}
