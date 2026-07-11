package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "os"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type WorkerStruct struct {
    mapf    func(string, string) []KeyValue
    reducef func(string, []string) string
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

	// Your worker implementation here.
	w := WorkerStruct{
        mapf:    mapf,
        reducef: reducef,
    }

	// uncomment to send the Example RPC to the master.
	w.CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (w *WorkerStruct) CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	fmt.Printf("reply.File %s\n", reply.File)

	file, err := os.Open(reply.File)
    if err != nil {
        log.Fatalf("cannot open %v", reply.File)
    }
    content, err := ioutil.ReadAll(file)
    if err != nil {
        log.Fatalf("cannot read %v", reply.File)
    }
    file.Close()

	kva := w.mapf(reply.File, string(content))
	_ = kva

	fmt.Printf("successfully did the map kva\n")
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
