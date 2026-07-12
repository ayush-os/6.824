package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	files []string
	idx int
	
	nTotMap int
	nDoneMap int
	
	nTotReduce int
	nInFlightReduces int
	nDoneReduces int
}

func (m *Master) GetTask(args *TaskArgs, reply *TaskReply) error {
	if args.FinishedMapTask {
		m.nDoneMap++
	} else if args.FinishedReduceTask {
		m.nDoneReduces++
		m.nInFlightReduces--
	}

	if m.nDoneMap < m.nTotMap {
		if m.idx < len(m.files) {
			reply.Action = Map
			reply.File = m.files[m.idx]
			reply.NReduce = m.nTotReduce
			reply.TaskID = m.idx
			m.idx++
		} else {
			reply.Action = Wait
		}
	} else {
		if m.nTotReduce == m.nDoneReduces {
			reply.Action = Shutdown
		} else if m.nTotReduce == (m.nDoneReduces + m.nInFlightReduces) {
			reply.Action = Wait
		} else {
			reply.Action = Reduce
			reply.NReduce = m.nTotReduce
			reply.NMap = m.nTotMap
			reply.TaskID = m.nInFlightReduces
			m.nInFlightReduces++
		}
	}
	
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.nTotReduce == m.nDoneReduces
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.files = files
	m.nTotMap = len(files)
	m.nTotReduce = nReduce

	m.server()
	return &m
}
