package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"

	"github.com/google/uuid"
)

var (
	workerID = uuid.New()
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		requestTaskReply, err := RequestTask()
		if err != nil {
			// What do we do, try again?
			fmt.Println(err)
			continue
		} else if requestTaskReply.TaskID < 0 {
			fmt.Println("No task available")
			continue
		}

		if requestTaskReply.TaskType == Map {
			// We need to do the map, and then put the intermediate key values
			// in the reduce buckets.
			kva := doMap(requestTaskReply.FileName, mapf)
			iFiles := make([]string, 0)
			for _, kv := range kva {
				reduceBucket := ihash(kv.Key) % requestTaskReply.NReduce
				filename := fmt.Sprintf("mr-%v-%v", requestTaskReply.TaskID, reduceBucket)
				iFiles = append(iFiles, filename)
				file, err := os.Create(filename)
				if err != nil {
					log.Fatalf("cannot create %v", filename)
				}
				enc := json.NewEncoder(file)
				err = enc.Encode(&kv)
				if err != nil {
					panic(err)
				}
			}
			if err := MapDone(iFiles); err != nil {
				// What do we do now?
				fmt.Println(err)
			}
		} else if requestTaskReply.TaskType == Reduce {
			// We need to read from all of the intermediate files. Can the coordinator
			// tell us the file names?
			for _, file := range requestTaskReply.IntermediateFiles {
				// Reduce and write to output files
				fmt.Println(file)
			}
		}
		time.Sleep(1 * time.Second)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func MapDone(intermediateFiles []string) {

	// declare an argument structure.
	args := MapDoneArgs{
		Success:           true,
		IntermediateFiles: intermediateFiles,
	}

	// declare a reply structure.
	reply := MapDoneReply{}

	ok := call("Coordinator.MapDone", &args, &reply)
	if ok {
		fmt.Println("Told coordinator map is done")
	} else {
		fmt.Printf("call failed!\n")
	}
}

func RequestTask() (*RequestTaskReply, error) {

	args := RequestTaskArgs{
		WorkerID: workerID.String(),
	}

	reply := RequestTaskReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		return &reply, nil
	} else {
		return nil, fmt.Errorf("RequestTask error. args: %v", args)
	}
}

func doMap(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return mapf(filename, string(content))
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
