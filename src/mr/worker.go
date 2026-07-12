package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"

	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type WorkerStruct struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	w := WorkerStruct{
		mapf:    mapf,
		reducef: reducef,
	}

	w.DoTasks()
}

func (w *WorkerStruct) DoTasks() {
	args := TaskArgs{}
	reply := TaskReply{}
	ok := call("Master.GetTask", &args, &reply)
	if !ok {
		return
	}

	for {
		if reply.Action == Map {
			ok = w.handleMapTask(&reply, &args)
		} else if reply.Action == Reduce {
			ok = w.handleReduceTask(&reply, &args)
		} else if reply.Action == Wait {
			time.Sleep(time.Second)
			args.FinishedMapTask = false
			args.FinishedReduceTask = false
			ok = call("Master.GetTask", &args, &reply)
		} else if reply.Action == Shutdown {
			break
		}

		if !ok {
			return
		}
	}
}

func (w *WorkerStruct) handleMapTask(reply *TaskReply, args *TaskArgs) bool {
	tmpFiles := make([]*os.File, reply.NReduce)
	encoders := make([]*json.Encoder, reply.NReduce)

	for i := 0; i < reply.NReduce; i++ {
		tmpName := fmt.Sprintf("mr-tmp-%d-%d", reply.TaskID, i)
		tmpFile, err := os.CreateTemp("", tmpName)
		if err != nil {
			log.Fatalf("cannot create temp file")
		}
		tmpFiles[i] = tmpFile
		encoders[i] = json.NewEncoder(tmpFile)
	}

	file, err := os.Open(reply.File)
	if err != nil {
		log.Fatalf("cannot open %v", reply.File)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.File)
	}
	file.Close()

	kva := w.mapf(reply.File, string(content))

	for _, kv := range kva {
		bucket := ihash(kv.Key) % reply.NReduce

		err := encoders[bucket].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode json")
		}
	}

	for i := 0; i < reply.NReduce; i++ {
		tmpFiles[i].Close()
		finalName := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
		os.Rename(tmpFiles[i].Name(), finalName)
	}

	args.FinishedMapTask = true
	args.FinishedReduceTask = false
	args.FinishedTaskID = reply.TaskID

	*reply = TaskReply{}
	return call("Master.GetTask", args, reply)
}

func (w *WorkerStruct) handleReduceTask(reply *TaskReply, args *TaskArgs) bool {
	var intermediate []KeyValue

	for m := 0; m < reply.NMap; m++ {
		filename := fmt.Sprintf("mr-%d-%d", m, reply.TaskID)

		file, err := os.Open(filename)
		if err != nil {
			continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break // End of file
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	tmpFile, err := os.CreateTemp("", "mr-tmp-*")
	if err != nil {
		log.Fatalf("cannot create temp file: %v", err)
	}

	tmpName := tmpFile.Name()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		// Collect all values for this specific key
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		// Call the user-defined Reduce function
		output := w.reducef(intermediate[i].Key, values)

		// Write the output in the required format
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tmpFile.Close()

	finalName := fmt.Sprintf("mr-out-%d", reply.TaskID)
	err = os.Rename(tmpName, finalName)
	if err != nil {
		log.Fatalf("cannot rename temp file to final output: %v", err)
	}

	args.FinishedReduceTask = true
	args.FinishedMapTask = false
	args.FinishedTaskID = reply.TaskID

	*reply = TaskReply{}
	return call("Master.GetTask", args, reply)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
