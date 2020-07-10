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

// Master struct encapsulates all info needed to run a master node.
type Master struct {
	sync.Mutex
	Files         []FileState
	ReduceTasks   []ReduceState
	NReduce       int
	Completed     bool
	CurrentStage  int // 0 -> Map, 1 -> Reduce
	FilledBuckets map[int]string
}

// FileState struct represents a file's state
type FileState struct {
	FileName string
	State    int // 0 -> no one is working on it , 1 -> a worker is currently handling it, 2 -> successfully handled
}

// ReduceState struct represents a reduce task's state
type ReduceState struct {
	TaskNumber int
	State      int // 0 -> unhandled , 1 -> currently being handled, 2-> successfully handled
}

// Your code here -- RPC handlers for the worker to call.

// GetTask is an RPC which workers use to request a task to work on.
func (m *Master) GetTask(req *GetTaskRequest, resp *GetTaskResponse) error {
	m.Lock()
	defer m.Unlock()
	if m.CurrentStage == 0 {
		// still in map state
		// find the next free task. first determine if there are any map tasks left.
		freeIdx := -1
		for i, f := range m.Files {
			if f.State == 0 {
				freeIdx = i
				break
			}
		}

		if freeIdx == -1 {
			// at this point, freeIdx == -1, that means all Map tasks have been
			// dished out to workers and are currently in-progress or have already
			// completed. but since m.CurrentStage == 0, meaning that we can't
			// start reduce stage yet, so we set tasktype to -1 which the worker
			// will regard as unhandled, meaning it will sleep and wait till
			// next task arrives.
			resp.TaskType = -1
			return nil
		}
		// found an unhandled map task. serve it to worker.
		resp.FileName = m.Files[freeIdx].FileName
		resp.TaskType = 0
		resp.NReduce = m.NReduce
		resp.TaskNumber = freeIdx
		resp.NMap = len(m.Files)
		m.Files[freeIdx].State = 1 // we change the state to in progress

		// start a go-routine which waits for 10 seconds and sets the file state
		// back to unhandled if the change is still in-progress. This means that the
		// worker is either too slow or crashed, which we will just handle this case
		// as re-issue the task to another worker.
		go func(fileIdx int) {
			time.Sleep(time.Second * 10)
			m.Lock()
			defer m.Unlock()
			if m.Files[fileIdx].State == 1 {
				log.Printf("GetTask goroutine-- worker servicing file with idx %v crashed. Reissuing map task.\n", fileIdx)
				m.Files[fileIdx].State = 0
			}
		}(freeIdx)
		return nil
	} else {
		// we are in reduce-stage
		// find the next free task.
		freeIdx := -1
		for i, f := range m.ReduceTasks {
			if f.State == 0 {
				freeIdx = i
				break
			}
		}

		if freeIdx == -1 {
			// at this point, freeIdx == -1, that means all reduce tasks have been
			// dished out to workers and are currently in-progress or have already
			// completed. No tasks to hand out.
			resp.TaskType = -1
			return nil
		}
		// found an unhandled reduce task. serve it to worker.
		resp.TaskType = 1
		resp.NReduce = m.NReduce
		resp.TaskNumber = m.ReduceTasks[freeIdx].TaskNumber
		resp.NMap = len(m.Files)
		m.ReduceTasks[freeIdx].State = 1 // we change the state to in progress

		// start a go-routine which waits for 10 seconds and sets the file state
		// back to unhandled if the change is still in-progress. This means that the
		// worker is either too slow or crashed, which we will just handle this case
		// as re-issue the task to another worker.
		go func(fileIdx int) {
			time.Sleep(time.Second * 10)
			m.Lock()
			defer m.Unlock()
			if m.ReduceTasks[fileIdx].State == 1 {
				log.Printf("GetTask goroutine-- worker servicing file with idx %v crashed. Reissuing reduce task.\n", fileIdx)
				m.ReduceTasks[fileIdx].State = 0
			}
		}(freeIdx)
		return nil
	}
}

// SignalCompleteTask is an RPC which workers use to signal that they have
// completed their assigned task.
func (m *Master) SignalCompleteTask(req *SignalCompleteTaskRequest, resp *SignalCompleteTaskResponse) error {
	m.Lock()
	defer m.Unlock()
	log.Printf("SignalCompleteTask: received req: %v\n", req)
	// for completed map task, we update the master's internal fileStates to
	// indicate that this particular file has been mapped. if all map tasks have
	// completed, we move to reduce task
	if req.TaskType == 0 {
		m.Files[req.TaskNumber].State = 2

		for k := range req.FilledBuckets {
			m.FilledBuckets[k] = "true"
		}

		completed := true
		for _, m := range m.Files {
			if m.State != 2 {
				completed = false
				break
			}
		}
		if completed {
			log.Println("SignalCompleteTask: changing stage to reduce")
			for k := range m.FilledBuckets {
				m.ReduceTasks = append(m.ReduceTasks, ReduceState{
					TaskNumber: k,
					State:      0,
				})
			}
			m.CurrentStage = 1
		}
	} else {
		// for completed reduce task, we update master's internal ReduceTasks
		// state to indicate that this reduce task has been completed. If all
		// reduce tasks have been completed, we set m.Completed to true.
		idx := -1
		for i, t := range m.ReduceTasks {
			if req.TaskNumber == t.TaskNumber {
				idx = i
				break
			}
		}
		m.ReduceTasks[idx].State = 2
		completed := true
		for _, r := range m.ReduceTasks {
			if r.State != 2 {
				completed = false
				break
			}
		}
		if completed {
			m.Completed = true
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
	// l, e := net.Listen("tcp", ":1234")
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
	m.Lock()
	defer m.Unlock()
	return m.Completed
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	fileStates := []FileState{}
	for _, f := range files {
		fileStates = append(fileStates, FileState{
			FileName: f,
			State:    0,
		})
	}

	m := Master{
		Files:         fileStates,
		NReduce:       nReduce,
		Completed:     false,
		ReduceTasks:   []ReduceState{},
		CurrentStage:  0,
		FilledBuckets: make(map[int]string),
	}
	m.server()
	return &m
}
