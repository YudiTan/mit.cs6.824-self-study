package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// GetTaskRequest represents the request argument for GetMapTask RPC
type GetTaskRequest struct {
}

// GetTaskResponse represents the response argument for GetMapTask RPC
type GetTaskResponse struct {
	FileName   string
	TaskType   int // 0 -> Map, 1 -> Reduce
	NReduce    int
	TaskNumber int
	NMap       int
}

// SignalCompleteTaskRequest represents the request argument for
// SignalCompleteTask RPC
type SignalCompleteTaskRequest struct {
	FileName      string
	TaskType      int // 0 -> Map, 1 -> Reduce
	TaskNumber    int
	FilledBuckets map[int]string
}

// SignalCompleteTaskResponse represents the response argument for
// SignalCompleteTask RPC
type SignalCompleteTaskResponse struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
