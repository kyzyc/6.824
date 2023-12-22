package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"io/ioutil"
	"strconv"
	// "sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		
	}
	
	task, taskid, nReduce := CallForTask()

	DoMapTask(task, taskid, nReduce, mapf)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// task - file name
func DoMapTask(task string, taskid int, nReduce int, mapf func(string, string) []KeyValue) {
	intermediate := []KeyValue{}

	file, err := os.Open(task)
	if err != nil {
		log.Fatalf("cannot open %v", task)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task)
	}
	file.Close()

	intermediate = mapf(task, string(content))

	// store intermediate to intermediate files
	// format mr-X-Y, X is Map task id, Y is the reduce task id
	
	// first sort the intermediate
	// sort.Sort(ByKey(intermediate))

	oname_prefix := "mr-" + strconv.Itoa(taskid)
	ofile := make([]*os.File, nReduce)

	for i := 0; i < nReduce; i++ {
		oname := oname_prefix + "-" + strconv.Itoa(i)
		ofile[i], _ = os.Create(oname)
	}

	for i := 0; i < len(intermediate); i++ {
		fileID := ihash(intermediate[i].Key) % nReduce
		fmt.Fprintf(ofile[fileID], "%v %v\n", intermediate[i].Key, intermediate[i].Value)
	}
}

func CallForTask() (string, int, int){
	args := Args{}
	reply := Reply{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		return reply.Task, reply.Taskid, reply.NReduce
	} else {
		fmt.Printf("call failed!\n")
		return "", 0, 0
	} 
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
