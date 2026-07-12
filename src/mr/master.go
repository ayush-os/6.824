package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

const (
	Todo TaskState = iota
	IP
	Done
)

type Task struct {
	File      string
	State     TaskState
	StartTime time.Time
}

type Master struct {
	mu sync.Mutex

	mapTasks    []Task
	reduceTasks []Task

	nTotMap  int
	nDoneMap int

	nTotReduce  int
	nDoneReduce int
}

func (m *Master) GetTask(args *TaskArgs, reply *TaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if args.FinishedMapTask && m.mapTasks[args.FinishedTaskID].State != Done {
		m.mapTasks[args.FinishedTaskID].State = Done
		m.nDoneMap++
	} else if args.FinishedReduceTask && m.reduceTasks[args.FinishedTaskID].State != Done {
		m.reduceTasks[args.FinishedTaskID].State = Done
		m.nDoneReduce++
	}

	var wait bool = true
	if m.nDoneMap < m.nTotMap {
		for i, task := range m.mapTasks {
			if (task.State == IP && time.Since(task.StartTime) > 10*time.Second) || task.State == Todo {
				task.State = IP
				task.StartTime = time.Now()

				reply.Action = Map
				reply.File = task.File
				reply.NReduce = m.nTotReduce
				reply.NMap = m.nTotMap
				reply.TaskID = i

				wait = false
			}
		}
	} else {
		if m.nTotReduce == m.nDoneReduce {
			reply.Action = Shutdown
			wait = false
		} else {
			for i, task := range m.reduceTasks {
				if (task.State == IP && time.Since(task.StartTime) > 10*time.Second) || task.State == Todo {
					task.State = IP
					task.StartTime = time.Now()

					reply.Action = Reduce
					reply.File = ""
					reply.NReduce = m.nTotReduce
					reply.NMap = m.nTotMap
					reply.TaskID = i

					wait = false
				}
			}
		}
	}
	if wait {
		reply.Action = Wait
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nTotReduce == m.nDoneReduce
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	for _, file := range files {
		m.mapTasks = append(m.mapTasks, Task{
			File:  file,
			State: Todo,
		})
	}
	for range nReduce {
		m.reduceTasks = append(m.reduceTasks, Task{
			State: Todo,
		})
	}
	m.nTotMap = len(m.mapTasks)
	m.nTotReduce = len(m.reduceTasks)

	m.server()
	return &m
}
