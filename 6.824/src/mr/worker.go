package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

// ByKey is for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// once a worker starts, it goes in an infinite loop and only exits if
	// RPC fails, which most likely means that the master has terminated and
	// hence there are not more tasks to run.
	for {
		task, err := GetTaskFromMaster()
		if err != nil {
			// something went wrong with the RPC call, we terminate this worker.
			log.Fatalf("Worker: %v", err.Error())
		}
		// now that we have the task, we can start processing it.
		switch task.TaskType {
		case 0: // map task
			log.Printf("Worker: received map task: %v\n", task)
			ProcessMap(mapf, task)
		case 1: // reduce task
			log.Printf("Worker: received reduce task: %v\n", task)
			ProcessReduce(reducef, task)
		default: // undefined task

		}
		// workers sometimes need to wait (i.e. reduces can't start until the
		// last map finishes)
		time.Sleep(time.Second * 1)
	}

}

// GetTaskFromMaster wraps an RPC call to get a task from master.
func GetTaskFromMaster() (GetTaskResponse, error) {
	req := GetTaskRequest{}
	resp := GetTaskResponse{}
	// send the RPC request, wait for the reply.
	callRes := call("Master.GetTask", &req, &resp)
	if !callRes {
		return resp, errors.New("GetTaskFromMaster-- Error calling Master.GetTask RPC")
	}
	return resp, nil
}

// ProcessMap handles applying the mapf to a task
func ProcessMap(mapf func(string, string) []KeyValue, task GetTaskResponse) {
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("ProcessMap: cannot open %v", task.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("ProcessMap: cannot read %v", task.FileName)
	}
	file.Close()
	filledBuckets := make(map[int]string)
	intermediateKVs := mapf(task.FileName, string(content))
	tempFiles := make(map[string]*os.File) // map of real name <-> tmp file
	// for each intermediate KV, we need to store them in their respective
	// buckets
	for _, kv := range intermediateKVs {
		bucket := ihash(kv.Key) % task.NReduce
		filledBuckets[bucket] = ""
		oname := fmt.Sprintf("mr-%v-%v", task.TaskNumber, bucket)
		var ofile *os.File
		// to prevent partially written files, use ioutil.tempfile and
		// os.rename strategy in the specs.
		if f, ok := tempFiles[oname]; ok {
			// a tempfile has already been created for this intermediate file
			ofile = f
		} else {
			// need to create a new tempfile
			ofile, err = ioutil.TempFile("", oname)
			if err != nil {
				log.Fatalf("ProcessMap: error creating temp file for intermediate KV: %v\n", err)
			}
			tempFiles[oname] = ofile
		}
		// // If the file doesn't exist, create it, or append to the file
		// ofile, err := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		// if err != nil {
		// 	log.Fatalf("ProcessMap: error creating output file for intermediate KV: %v\n", err)
		// }
		enc := json.NewEncoder(ofile)
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatalf("ProcessMap: error writing kv to file: %v\n", err)
		}
		// err = ofile.Close()
		// if err != nil {
		// 	log.Fatalf("ProcessMap: error closing file: %v\n", err)
		// }

	}
	// atomically rename all temp files to actual output file names
	for k, v := range tempFiles {
		v.Close()
		os.Rename(v.Name(), k)
	}

	// now that we have completed the map task, we need to notify the master
	sigReq := SignalCompleteTaskRequest{
		TaskType:      0,
		TaskNumber:    task.TaskNumber,
		FilledBuckets: filledBuckets,
	}
	sigResp := SignalCompleteTaskResponse{}
	call("Master.SignalCompleteTask", &sigReq, &sigResp)
}

// ProcessReduce handles applying the reducef to a task
func ProcessReduce(reducef func(string, []string) string, task GetTaskResponse) {
	// we try to read all the intermediate files belonging to this task number
	kvMap := make(map[string][]string)
	for i := 0; i < task.NMap; i++ {
		fName := fmt.Sprintf("mr-%v-%v", i, task.TaskNumber)
		file, err := os.Open(fName)
		if err != nil {
			log.Fatalf("ProcessReduce: error opening: %v\n", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				break // done decoding
			} else if err != nil {
				file.Close()
				log.Fatalf("ProcessReduce: error decoding intermediate KV: %v\n", err)
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
		file.Close()
	}
	oname := fmt.Sprintf("mr-out-%v", task.TaskNumber)
	ofile, err := ioutil.TempFile("", oname)
	if err != nil {
		log.Fatalf("ProcessReduce: Failed to create tmp output file: %v\n", err)
	}

	for key, values := range kvMap {
		red := reducef(key, values)
		fmt.Fprintf(ofile, "%v %v\n", key, red)
	}
	// now that we have written everything to tmp file successfully, we
	// atomically rename it to actual output name
	os.Rename(ofile.Name(), oname)
	defer ofile.Close()

	// now that we have completed the reduce task, we need to notify the master
	sigReq := SignalCompleteTaskRequest{
		TaskType:   1,
		TaskNumber: task.TaskNumber,
	}
	sigResp := SignalCompleteTaskResponse{}
	call("Master.SignalCompleteTask", &sigReq, &sigResp)

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
