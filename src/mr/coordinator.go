package mr

import (
	// "fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type TaskStatus int
type TaskType	int

const (
	IDLE = 0
	IN_PROCESS = 1
	DONE = 2
)

const (
	NOTASKYET = 0	// represent no task is doable, maybe because map tasks not finished
	MAP = 1
	REDUCE = 2
)

type Coordinator struct {
	// Your definitions here.
	files []string	// store input files
	nReduce int
	mapDone bool	// denote whether map tasks all have done
	reduceDone bool // denote whether reduce tasks all have done

	mapTaskMeta MapTaskMeta
	reduceTaskMeta ReduceTaskMeta
}

type MapTaskMeta struct {
	taskStatus []TaskStatus		// task status for map
}

type ReduceTaskMeta struct {
	taskStatus []TaskStatus		// task status for reduce
	intermidateLocation [][]string
}

// Your code here -- RPC handlers for the worker to call.
// ask for task to do
func (c *Coordinator) AssignTask(args *Args, reply *Reply) error {
	if !c.mapDone {
		// should assign map task
		reply.TaskType = MAP
		reply.Task = c.files[0]
		reply.Taskid = 0
		reply.NReduce = c.nReduce
		c.mapDone = true
	} else {
		// should assign reduce task
		reply.TaskType = REDUCE
		reply.ReduceID = 0
		reply.ReduceTaskLocation = []string {
			"mr-0-0",
		}
		c.reduceDone = true
	}
	return nil
}

// tell the cordinator that have finished the task
func (c *Coordinator) DoneMapTask(args *Args, reply *Reply) error {
	return nil
}

// tell the cordinator that have finished the task
func (c *Coordinator) DoneReduceTask(args *Args, reply *Reply) error {
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if (c.mapDone && c.reduceDone) {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files, nReduce, false, false, MapTaskMeta{}, ReduceTaskMeta{}}

	// Your code here.
	// c.files = make([]string, len(files))
	// copy(c.files, files)		// first store input files

	c.server()
	return &c
}
