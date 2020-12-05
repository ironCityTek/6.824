package mr

import (
	"fmt"
	"log"
	"net/rpc"
)

func call(address string, rpcname string, args interface{}, reply interface{}) bool {
	c, errC := rpc.Dial("unix", address)
	if errC != nil {
		log.Fatal("dialing:", errC)
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
