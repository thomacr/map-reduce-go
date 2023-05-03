package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"

	"github.com/google/uuid"
)

type TaskState uint8

const (
	Idle TaskState = iota
	InProgress
	Completed
)

// Let's keep track of tasks using this type.
// Unexported for now.
type task struct {
	taskState TaskState
	workerID  uuid.UUID
}

type Coordinator struct {
	// We need a list of map tasks and reduce tasks. Do we use a map?
	// An array will suffice for now. Actually, use a map for the
	// map tasks, and we can use the file name for the task as a key.
	mapTasks    map[string]*task
	reduceTasks []*task
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	// A worker has requested a task.
	for file, task := range c.mapTasks {
		if task.taskState == Idle {
			reply.FileName = file
			reply.TaskType = Map
			task.workerID = uuid.MustParse(args.WorkerID)
			return nil
		}
	}
	fmt.Println("No available tasks")
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:    make(map[string]*task),
		reduceTasks: make([]*task, 0, nReduce),
	}

	// Let's create a map task for each file.
	for _, file := range files {
		c.mapTasks[file] = &task{}
	}

	c.server()
	return &c
}
