package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
)

// Master struct encapsulates all info needed to run a master node.
type Master struct {
	// Your definitions here.
	// things to keep track: 1) list of workers so we can periodically ping them
	// to check on status 2) list of files that the client wants us to process
	// 3) number of reduce tasks that user specified
	Files   []string
	NReduce int
}

// Your code here -- RPC handlers for the worker to call.

// GetTask is an RPC which workers use to request a task to work on.
func (m *Master) GetTask(req *GetTaskRequest, resp *GetTaskResponse) error {
	resp.FileName = m.Files[0]
	resp.TaskType = 0
	resp.NReduce = m.NReduce
	resp.MapTaskNumber = 0
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	// sockname := masterSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
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
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		Files:   files,
		NReduce: nReduce,
	}

	// Your code here.

	m.server()
	return &m
}
