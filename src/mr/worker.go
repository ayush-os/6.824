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

type worker struct {
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

	w := worker{
		mapf:    mapf,
		reducef: reducef,
	}

	w.run()
}

func (w *worker) run() {
	args := TaskArgs{FinishedTaskID: -1}

	for {
		reply := TaskReply{}
		if ok := call("Master.GetTask", &args, &reply); !ok {
			return
		}

		switch reply.Action {
		case Map:
			w.doMap(&reply)
			args = TaskArgs{FinishedAction: Map, FinishedTaskID: reply.TaskID}
		case Reduce:
			w.doReduce(&reply)
			args = TaskArgs{FinishedAction: Reduce, FinishedTaskID: reply.TaskID}
		case Wait:
			time.Sleep(time.Second)
			args = TaskArgs{FinishedTaskID: -1}
		case Shutdown:
			return
		}
	}
}

func (w *worker) doMap(reply *TaskReply) {
	tmpFiles := make([]*os.File, reply.NReduce)
	encoders := make([]*json.Encoder, reply.NReduce)

	for i := 0; i < reply.NReduce; i++ {
		tmpFile, err := os.CreateTemp(".", fmt.Sprintf("mr-tmp-%d-%d-*", reply.TaskID, i))
		if err != nil {
			log.Fatalf("cannot create temp file: %v", err)
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
		if err := encoders[bucket].Encode(&kv); err != nil {
			log.Fatalf("cannot encode json: %v", err)
		}
	}

	for i := 0; i < reply.NReduce; i++ {
		tmpFiles[i].Close()
		finalName := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
		if err := os.Rename(tmpFiles[i].Name(), finalName); err != nil {
			log.Fatalf("cannot rename temp file to %v: %v", finalName, err)
		}
	}
}

func (w *worker) doReduce(reply *TaskReply) {
	var intermediate []KeyValue

	for m := 0; m < reply.NMap; m++ {
		filename := fmt.Sprintf("mr-%d-%d", m, reply.TaskID)

		file, err := os.Open(filename)
		if err != nil {
			log.Printf("warning: could not open intermediate file %v: %v", filename, err)
			continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break // end of file
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	tmpFile, err := os.CreateTemp(".", "mr-tmp-out-*")
	if err != nil {
		log.Fatalf("cannot create temp file: %v", err)
	}

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
	if err := os.Rename(tmpFile.Name(), finalName); err != nil {
		log.Fatalf("cannot rename temp file to %v: %v", finalName, err)
	}
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
