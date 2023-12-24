package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type Args struct {
	
}

// Add your RPC definitions here.
type MapArgs struct {
	// some message that will send to coordinator
	Taskid int
	Intermidate []string
}

type ReduceArgs struct {
	// some message that will send to coordinator
	Taskid int
}

type Reply struct {
	// Task Type
	TaskType int
	Taskid  int						// id of task, for distinguish worker
	// Meta information for map task
	Task string						// task file name
	NReduce int						// number of reduce
	// Meta information for reduce task
	ReduceTaskLocation []string 	// location of intermidate files
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
