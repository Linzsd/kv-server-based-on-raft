package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskArgs struct {
}

type Task struct {
	TaskType   TaskType
	TaskId     int
	ReducerNum int
	FileSlice  []string
	State      TaskState
	StartTime  time.Time // 任务开始时间
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaittingTask
	ExitTask
)

type TaskState int

const (
	Working TaskState = iota
	Waiting
	Done
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
