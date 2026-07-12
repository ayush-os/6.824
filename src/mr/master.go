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
	InProgress
	Done
)

// taskTimeout is how long the master waits before assuming a worker
// that was given a task has died, and reassigning that task.
const taskTimeout = 10 * time.Second

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

	m.markFinished(args)

	if m.nDoneMap < m.nTotMap {
		if i, ok := assign(m.mapTasks); ok {
			reply.Action = Map
			reply.File = m.mapTasks[i].File
			reply.NReduce = m.nTotReduce
			reply.NMap = m.nTotMap
			reply.TaskID = i
			return nil
		}
		reply.Action = Wait
		return nil
	}

	if m.nDoneReduce == m.nTotReduce {
		reply.Action = Shutdown
		return nil
	}

	if i, ok := assign(m.reduceTasks); ok {
		reply.Action = Reduce
		reply.NReduce = m.nTotReduce
		reply.NMap = m.nTotMap
		reply.TaskID = i
		return nil
	}

	reply.Action = Wait
	return nil
}

func (m *Master) markFinished(args *TaskArgs) {
	if args.FinishedTaskID < 0 {
		return
	}
	switch args.FinishedAction {
	case Map:
		if m.mapTasks[args.FinishedTaskID].State != Done {
			m.mapTasks[args.FinishedTaskID].State = Done
			m.nDoneMap++
		}
	case Reduce:
		if m.reduceTasks[args.FinishedTaskID].State != Done {
			m.reduceTasks[args.FinishedTaskID].State = Done
			m.nDoneReduce++
		}
	}
}

func assign(tasks []Task) (int, bool) {
	for i := range tasks {
		t := &tasks[i]
		if t.State == Todo || (t.State == InProgress && time.Since(t.StartTime) > taskTimeout) {
			t.State = InProgress
			t.StartTime = time.Now()
			return i, true
		}
	}
	return 0, false
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
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
