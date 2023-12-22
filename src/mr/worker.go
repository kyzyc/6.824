package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"io/ioutil"
	"strconv"
	"time"
	"encoding/json"
	"sort"
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
	reply := Reply{}
	for {
		ok := CallForTask(&reply)
		if (!ok) {
			break
		}
		switch reply.TaskType {
		case MAP:
			DoMapTask(reply.Task, reply.Taskid, reply.NReduce, mapf)
			break
		case REDUCE:
			DoReduceTask(reply.Taskid, reply.ReduceTaskLocation, reducef)
			break
		case IDLE:
			time.Sleep(time.Second)
			break
		default:
			panic("unknown task type!")
		}
	}
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
	oname_prefix := "mr-" + strconv.Itoa(taskid)
	ofile := make([]*os.File, nReduce)

	for i := 0; i < nReduce; i++ {
		oname := oname_prefix + "-" + strconv.Itoa(i)
		ofile[i], _ = os.Create(oname)
	}

	enc := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		enc[i] = json.NewEncoder(ofile[i])
	}

	for _, kv := range intermediate {
		fileID := ihash(kv.Key) % nReduce
		err := enc[fileID].Encode(&kv)
		if err != nil {
			panic("Error while write to intermidate file!")
		}
	}
}

func DoReduceTask(taskid int, intermidateLocation []string, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for _, filename := range intermidateLocation {
		file, err := os.Open(filename)
		dec := json.NewDecoder(file)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		kva := []KeyValue{}
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
			  break
			}
			kva = append(kva, kv)
		}
		file.Close()

		intermediate = append(intermediate, kva...)
	}

	sort.Sort(ByKey(intermediate))


	oname := "mr-out-" + strconv.Itoa(taskid)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

func CallForTask(reply *Reply) bool {
	args := Args{}

	return call("Coordinator.AssignTask", &args, &reply)
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
