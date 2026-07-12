package mr

//
// RPC definitions.
//

import "os"
import "strconv"

type TaskAction int

const (
	Map TaskAction = iota
	Reduce
	Wait
	Shutdown
)

type TaskArgs struct {
	WorkerID int

	FinishedMapTask bool
	FinishedReduceTask bool
}

type TaskReply struct {
	Action TaskAction

	File string
	NReduce int
	NMap int
	TaskID int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
