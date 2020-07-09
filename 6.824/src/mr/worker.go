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
	"sort"
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
		// note: this is a blocking call - the master will only respond when it
		// has a new task
		task, err := GetTaskFromMaster()
		if err != nil {
			// something went wrong with the RPC call, we terminate this worker.
			log.Fatalf("Worker: %v", err.Error())
		}
		// now that we have the task, we can start processing it.
		switch task.TaskType {
		case 0: // map task
			ProcessMap(mapf, task)
		case 1: // reduce task
			ProcessReduce(reducef, task)
		default: // undefined task
			log.Fatalf("Worker: Undefined TaskType: %v, exiting worker\n", task.TaskType)
		}
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
	intermediateKVs := mapf(task.FileName, string(content))
	// for each intermediate KV, we need to store them in their respective
	// buckets
	for _, kv := range intermediateKVs {
		bucket := ihash(kv.Key) % task.NReduce
		oname := fmt.Sprintf("mr-%v-%v", task.TaskNumber, bucket)
		// If the file doesn't exist, create it, or append to the file
		ofile, err := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("ProcessMap: error creating output file for intermediate KV: %v\n", err)
		}
		enc := json.NewEncoder(ofile)
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatalf("ProcessMap: error writing kv to file: %v\n", err)
		}
		err = ofile.Close()
		if err != nil {
			log.Fatalf("ProcessMap: error closing file: %v\n", err)
		}

	}
	// TODO: once complete, we need to notify master that we have completed this
	// map task.
}

// ProcessReduce handles applying the reducef to a task
func ProcessReduce(reducef func(string, []string) string, task GetTaskResponse) {
	// we first try to read and decode the intermediate KV
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("ProcessReduce: cannot open %v", task.FileName)
	}
	dec := json.NewDecoder(file)
	kva := []KeyValue{}
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err == io.EOF {
			break // done decoding
		} else if err != nil {
			file.Close()
			log.Fatalf("ProcessReduce: error decoding intermediate KV: %v\n", err)
		}
		kva = append(kva, kv)
	}
	file.Close()

	// sort the intermediate values by key
	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%v", task.TaskNumber)
	ofile, _ := os.Create(oname)

	// call Reduce on each distinct key in intermediate values,
	// and print the result to output file.
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()

	// TODO: notify master that reduce task has been completed

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := masterSock()
	// c, err := rpc.DialHTTP("unix", sockname)
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
