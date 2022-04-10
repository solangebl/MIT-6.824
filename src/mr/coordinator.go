package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const MAP = "map"
const REDUCE = "reduce"
const EXIT = "exit"

type Coordinator struct {
	// Your definitions here.
	NReduce   int
	Files     []string
	InProcess []string
}

var mu sync.Mutex
var mapCount = 1

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {

	// TODO: if both files and inprocess are empty, move on to reduce
	mu.Lock()
	defer mu.Unlock()

	// TODO: if both empty move on to reduce
	if len(c.Files) == 0 && len(c.InProcess) == 0 {
		reply.Task = EXIT
	}
	if len(c.Files) > 0 {

		// TODO: add filename to inProcess
		reply.Filename = c.Files[0]
		reply.Task = MAP
		reply.TaskNum = mapCount
		reply.Nreduce = c.NReduce
		mapCount += 1
		c.Files = c.Files[1:]
		fmt.Printf("new files: %v\n", c.Files)
		//c.InProcess = append(c.InProcess.append, c.Files)
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// TODO: return true when mapreduce is done

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Files = files
	c.NReduce = nReduce

	fmt.Printf("Files: %v\n", c.Files)
	fmt.Printf("Files len: %v\n", len(c.Files))

	c.server()
	return &c
}
