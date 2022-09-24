package mr

import (
	"log"
	"net/rpc"
	"os"
	"strconv"
)

type Empty struct {
}

type RegisterWorkerReply struct {
	WorkerID int
}

type RequestTaskArgs struct {
	WorkerID int
}

type CompleteTaskArgs struct {
	TaskID   int
	WorkerID int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func call(rpcName string, args interface{}, reply interface{}) (bool, error) {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockName := coordinatorSock()
	client, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		return false, err
	}
	defer client.Close()

	err = client.Call(rpcName, args, reply)
	if err == nil {
		return true, nil
	}

	log.Println(err)
	return false, nil
}
