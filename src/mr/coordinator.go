package mr

import (
	// "fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
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

	// Lock field
	mapTaskMetaLock sync.Mutex
	reduceTaskMetaLock sync.Mutex
	mapDoneLock sync.Mutex
	reduceDoneLock sync.Mutex
}

type MapTaskMeta struct {
	taskStatus []TaskStatus		// task status for map
	taskBeginTime []time.Time
}

type ReduceTaskMeta struct {
	taskStatus []TaskStatus		// task status for reduce
	taskBeginTime []time.Time
	intermidateLocation [][]string
}

func (c *Coordinator) FindMapTaskToDo() (int, bool) {
	allDone := true
	taskNum := len(c.files)
	c.mapTaskMetaLock.Lock()
	for i := 0; i < taskNum; i++ {
		if c.mapTaskMeta.taskStatus[i] == IDLE {
			c.mapTaskMetaLock.Unlock()
			return i, false
		} else if c.mapTaskMeta.taskStatus[i] == IN_PROCESS {
			allDone = false
			timeGone := time.Now().Sub(c.mapTaskMeta.taskBeginTime[i])
			if timeGone > (10 * time.Second) {
				c.mapTaskMetaLock.Unlock()
				return i, false
			}
		}
	}
	
	c.mapTaskMetaLock.Unlock()
	return -1, allDone
}

func (c *Coordinator) FindReduceTaskToDo() (int, bool) {
	allDone := true
	taskNum := c.nReduce
	c.reduceTaskMetaLock.Lock()
	for i := 0; i < taskNum; i++ {
		if c.reduceTaskMeta.taskStatus[i] == IDLE {
			c.reduceTaskMetaLock.Unlock()
			return i, false
		} else if c.reduceTaskMeta.taskStatus[i] == IN_PROCESS {
			allDone = false
			timeGone := time.Now().Sub(c.reduceTaskMeta.taskBeginTime[i])
			if timeGone > (10 * time.Second) {
				c.reduceTaskMetaLock.Unlock()
				return i, false
			}
		}
	}
	c.reduceTaskMetaLock.Unlock()
	return -1, allDone
}

// Your code here -- RPC handlers for the worker to call.
// ask for task to do
func (c *Coordinator) AssignTask(args *Args, reply *Reply) error {
	c.mapDoneLock.Lock()
	if !c.mapDone {
		taskIndex, allDone := c.FindMapTaskToDo()
		if taskIndex == -1 {
			// map task all have done
			reply.TaskType = NOTASKYET
			if allDone == true {
				c.mapDone = true	
			}
		} else {
			c.mapTaskMetaLock.Lock()
			reply.TaskType = MAP
			reply.Taskid = taskIndex
			reply.NReduce = c.nReduce
			reply.Task = c.files[taskIndex]
			c.mapTaskMeta.taskStatus[taskIndex] = IN_PROCESS
			c.mapTaskMeta.taskBeginTime[taskIndex] = time.Now()
			c.mapTaskMetaLock.Unlock()
		}
	} else {
		taskIndex, allDone := c.FindReduceTaskToDo()
		if taskIndex == -1 {
			// map task all have done
			reply.TaskType = NOTASKYET
			if allDone == true {
				c.reduceDoneLock.Lock()
				c.reduceDone = true
				c.reduceDoneLock.Unlock()
			}
		} else {
			c.reduceTaskMetaLock.Lock()
			reply.TaskType = REDUCE
			reply.Taskid = taskIndex
			reply.ReduceTaskLocation = c.reduceTaskMeta.intermidateLocation[taskIndex]
			c.reduceTaskMeta.taskStatus[taskIndex] = IN_PROCESS
			c.reduceTaskMeta.taskBeginTime[taskIndex] = time.Now()
			c.reduceTaskMetaLock.Unlock()
		}
	}
	c.mapDoneLock.Unlock()
	return nil
}

// tell the cordinator that have finished the task
func (c *Coordinator) DoneMapTask(args *MapArgs, reply *Reply) error {
	intermidate := args.Intermidate

	c.reduceTaskMetaLock.Lock()
	for i := 0; i < c.nReduce; i++ {
		c.reduceTaskMeta.intermidateLocation[i] = 
			append(c.reduceTaskMeta.intermidateLocation[i], intermidate[i])
	}
	c.reduceTaskMetaLock.Unlock()

	c.mapTaskMetaLock.Lock()
	c.mapTaskMeta.taskStatus[args.Taskid] = DONE
	c.mapTaskMetaLock.Unlock()

	return nil
}

// tell the cordinator that have finished the task
func (c *Coordinator) DoneReduceTask(args *ReduceArgs, reply *Reply) error {
	c.reduceTaskMetaLock.Lock()
	c.reduceTaskMeta.taskStatus[args.Taskid] = DONE
	c.reduceTaskMetaLock.Unlock()
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
	c.mapDoneLock.Lock()
	c.reduceDoneLock.Lock()
	if (c.mapDone && c.reduceDone) {
		ret = true
	}
	c.reduceDoneLock.Unlock()
	c.mapDoneLock.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files, nReduce, false, false, MapTaskMeta{}, ReduceTaskMeta{},
                     sync.Mutex{}, sync.Mutex{}, sync.Mutex{}, sync.Mutex{}}

	// Your code here.
	taskNum := len(files)
	c.mapTaskMeta.taskStatus = make([]TaskStatus, taskNum)
	c.mapTaskMeta.taskBeginTime = make([]time.Time, taskNum)
	c.reduceTaskMeta.taskStatus = make([]TaskStatus, nReduce)
	c.reduceTaskMeta.taskBeginTime = make([]time.Time, nReduce)
	c.reduceTaskMeta.intermidateLocation = make([][]string, nReduce)

	c.server()
	return &c
}
