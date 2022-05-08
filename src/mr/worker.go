package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// Your worker implementation here.
	// declare an argument structure.
	args := TaskArgs{}

	// declare a reply structure.
	reply := TaskReply{}
	for getTask(args, &reply) != false {

		fmt.Printf("reply.Task %v\n", reply.Task)
		fmt.Printf("reply.Filename %v\n", reply.Filename)
		if reply.Task == "exit" {
			return
		}

		switch reply.Task {
		case "exit":
			return
		case "wait":
			time.Sleep(5 * time.Second)
		case "map":
			var intermediate = runMap(reply.Filename, mapf)
			// TODO: Partition intermediate into k => []kv so we save once to file
			err := saveIntermediate(intermediate, reply.TaskNum, reply.Nreduce)
			if err != nil {
				fmt.Printf("Save intermediate error %v", err)
			}
			_ = mapDone(reply.TaskNum)
		case "reduce":
			fmt.Printf("reduce task %v", reply.TaskNum)
			runReduce(reply.TaskNum, reducef)
		}

		reply = TaskReply{}
	}

	// uncomment to send the Example RPC to the coordinator.

}

func runMap(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	//fmt.Printf("Map on %v: %v\n", filename, kva)
	return kva
}

func runReduce(rNumber int, reducef func(string, []string) string) {

	oname := "mr-out-" + fmt.Sprint(rNumber)
	ofile, _ := os.Create(oname)

	intermediate := mergeIntermediateValues(rNumber)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

func saveIntermediate(intermediate []KeyValue, taskNum int, nReduce int) error {

	files := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		file, err := os.Create("mr-" + fmt.Sprint(taskNum) + "-" + fmt.Sprint(i))
		if err != nil {
			return err
		}
		files[i] = file
	}

	var rTask int
	kvParted := make([][]KeyValue, nReduce)
	for i := 0; i < len(intermediate); i++ {
		rTask = ihash(intermediate[i].Key) % nReduce
		kvParted[rTask] = append(kvParted[rTask], intermediate[i])
	}

	for key, v := range kvParted {
		err := encodeAndSave(v, "mr-"+fmt.Sprint(taskNum)+"-"+fmt.Sprint(key))
		if err != nil {
			return err
		}
	}
	return nil

}

func encodeAndSave(v []KeyValue, filename string) error {

	sort.Sort(ByKey(v))

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(file)
	err = enc.Encode(&v)
	if err != nil {
		return err
	}
	file.Close()

	return nil
}

func mergeIntermediateValues(i int) []KeyValue {

	var intermediate []KeyValue
	regexn := `mr-\d+-` + fmt.Sprint(i)

	path, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Fatalf(err.Error())
		}
		filename := info.Name()
		obj, _ := regexp.Match(regexn, []byte(filename))
		if obj == true {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			dec := json.NewDecoder(file)
			for {
				var kv []KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv...)
			}
		}
		return nil
	})

	sort.Sort(ByKey(intermediate))

	return intermediate
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

func getTask(args TaskArgs, reply *TaskReply) bool {

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetTask", &args, reply)
	if ok {
		// reply.Y should be a filename.
		fmt.Printf("reply.Filename %v\n", reply.Filename)
	} else {
		fmt.Printf("call failed!\n")
	}

	return ok
}

func mapDone(taskNum int) bool {

	// declare an argument structure.
	args := TaskArgs{taskNum}
	// declare a reply structure.
	reply := TaskReply{}
	ok := call("Coordinator.TaskDone", &args, &reply)
	if !ok {
		fmt.Printf("mapDone call failed!\n")
	}

	return ok
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
