package mr

import (
	"fmt"
	"log"
	"net/rpc"
)

func call(address string, rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := masterSock()

	fmt.Println(fmt.Sprintf("--@1 %v", address))
	fmt.Println(fmt.Sprintf("--@2 %v", rpcname))
	fmt.Println(fmt.Sprintf("--@3 %v", args))

	c, errC := rpc.Dial("unix", address)
	if errC != nil {
		log.Fatal("dialing:", errC)
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	// runtime.Breakpoint()
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
