package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

const (
	RequestTask = iota
	ReportTask
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	nReduce int
	ID      int
}

type ByKey []KeyValue

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

func (w *worker) registerCall() {
	reply := RegisterReply{}
	args := NullArgs{}

	//fmt.Printf("Worker send register request.\n")

	call("Coordinator.RegisterWorker", &args, &reply)
	w.ID = reply.AssignedID
	w.nReduce = reply.NReduce

	//fmt.Printf("Worker registered. ID: %v\n", w.ID)
}

func (w *worker) RequestTask() (Task, bool) {
	args := RequestTaskArgs{w.ID}
	reply := RequestTaskReply{}

	call("Coordinator.AssignTask", &args, &reply)
	return reply.Rtask, reply.AllDone
}

//
// deliver a task to map or reduce functions
//
func (w *worker) deliverTask(task Task) {
	if task.TaskType == MapTask {
		for w.doMapTask(task) == false {
		}
	} else {
		for w.doReduceTask(task) == false {
		}
	}
}

//
// Excute map task
//
func (w *worker) doMapTask(task Task) bool {

	var enc []*json.Encoder
	for i := 0; i < w.nReduce; i++ {
		fileName := fmt.Sprintf("mr-%v-%v", task.TaskIndex, i)
		tmpFile, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
			return false
		}
		enc = append(enc, json.NewEncoder(tmpFile))
	}

	for _, filename := range task.FileNames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return false
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
			return false
		}
		file.Close()
		kva := w.mapf(filename, string(content))

		for _, kv := range kva {
			reduceID := ihash(kv.Key) % w.nReduce
			enc[reduceID].Encode(&kv)
			if err != nil {
				log.Fatalf("Json Encode error")
				return false
			}
		}
	}
	return true
}

//
// Excute reduce task
//
func (w *worker) doReduceTask(task Task) bool {
	intermediate := []KeyValue{}
	// fmt.Printf("Start doing reduce task\n")
	for _, filename := range task.FileNames {
		file, err := os.OpenFile(filename, os.O_RDONLY, 0400)
		if err != nil {
			log.Fatalf("Cannot open %v", filename)
			return false
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", task.TaskIndex)
	ofile, _ := os.Create(oname)

	// fmt.Printf("Start Writing to the output file\n")

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
		output := w.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	return true
}

func (w *worker) reportTask(task Task) {
	args := ReportTaskArgs{task, w.ID}
	reply := NullReply{}

	call("Coordinator.FinishTask", &args, &reply)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	w := worker{mapf, reducef, 0, 0}
	w.registerCall()

	for {
		task, finished := w.RequestTask()

		if finished {
			break
		}
		w.deliverTask(task)
		w.reportTask(task)
		time.Sleep(time.Second)
	}
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
