package mr

import (
	"fmt"
	"log"
	"net/rpc"
)

func call(address string, rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := masterSock()

	fmt.Println(fmt.Sprintf("--@ %v", args))

	c, err := rpc.DialHTTP("unix", address)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	// runtime.Breakpoint()
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
