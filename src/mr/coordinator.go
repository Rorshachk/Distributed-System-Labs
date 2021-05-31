package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	ScheduleInterval = time.Millisecond * 100
	RunningTimeout   = time.Second * 10
)

const (
	DoingMap = iota
	DoingReduce
	Finished
)

const (
	TaskIndle = iota
	TaskInQueue
	TaskRunning
	TaskFinished
)

const (
	MapTask = iota
	ReduceTask
)

type Coordinator struct {
	// Your definitions here.
	Phase     int
	workerNum int
	nMap      int
	nReduce   int
	TaskList  []Task
	TaskQueue chan *Task
	mu        sync.Mutex
	done      bool
	workers   int
}

type Task struct {
	FileNames  []string
	TaskStatus int
	TaskType   int
	TaskIndex  int
	StartTime  time.Time
	workerID   int
}

func Max(x, y int) int {
	if x < y {
		return y
	} else {
		return x
	}
}

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

func (c *Coordinator) RegisterWorker(args *NullArgs, reply *RegisterReply) error {
	//fmt.Printf("Get worker registeration request. Acquiring the lock...\n")
	c.mu.Lock()
	defer c.mu.Unlock()
	//fmt.Printf("Lock acquired.\n")
	reply.AssignedID = c.workerNum
	c.workerNum++
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) AssignTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	task := <-c.TaskQueue
	c.mu.Lock()
	defer c.mu.Unlock()
	//if task is zero, then the channel is closed
	if task == nil {
		reply.AllDone = true
		return nil
	}

	task.TaskStatus = TaskRunning
	task.StartTime = time.Now()
	task.workerID = args.WorkerID
	reply.Rtask = *task

	// if c.Phase == DoingReduce {
	// 	fmt.Printf("Assign a reduce task\n")
	// }
	return nil
}

func (c *Coordinator) FinishTask(args *ReportTaskArgs, reply *NullReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	task := args.Rtask
	if args.WorkerID != c.TaskList[task.TaskIndex].workerID {
		return nil
	}
	c.TaskList[task.TaskIndex].TaskStatus = TaskFinished
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

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

func (c *Coordinator) allTaskFinished() bool {
	for _, task := range c.TaskList {
		if task.TaskStatus != TaskFinished {
			return false
		}
	}

	return true
}

func (c *Coordinator) schedule() {
	for {
		//fmt.Printf("Schedule the task, lock the Coordinator.\n")
		c.mu.Lock()
		if c.allTaskFinished() {
			//Start doing reduce task
			if c.Phase == DoingMap {
				// fmt.Printf("Initializing Reduce Phase..\n")
				c.Phase = DoingReduce
				c.TaskList = nil
				for i := 0; i < c.nReduce; i++ {
					//fmt.Printf("Reach Here? \n")
					var fileNames = []string{}
					for j := 0; j < c.nMap; j++ {
						fileName := fmt.Sprintf("mr-%v-%v", j, i)
						fileNames = append(fileNames, fileName)
					}
					c.TaskList = append(c.TaskList, Task{fileNames, TaskIndle, ReduceTask, i, time.Time{}, 0})
				}
			} else {
				close(c.TaskQueue)
				//wait for workers to be notified that the channel are closed so they exits normally
				time.Sleep(time.Second * 5)
				c.done = true
				c.mu.Unlock()
				break
			}

		}
		for idx := range c.TaskList {
			//fmt.Printf("Task?\n")
			task := &c.TaskList[idx]
			switch task.TaskStatus {
			case TaskIndle:
				task.TaskStatus = TaskInQueue
				c.TaskQueue <- task
			case TaskRunning:
				if time.Now().Sub(task.StartTime) > RunningTimeout {
					task.TaskStatus = TaskIndle
				}
			case TaskFinished:
			case TaskInQueue:
			}
		}

		//fmt.Printf("Schedule finish. Unlock the coordinator.\n")
		c.mu.Unlock()
		time.Sleep(ScheduleInterval)
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce
	c.nMap = len(files)
	c.done = false

	// Your code here.
	c.Phase = DoingMap
	for i, name := range files {
		task := Task{[]string{name}, TaskIndle, MapTask, i, time.Time{}, 0}
		c.TaskList = append(c.TaskList, task)
	}
	c.TaskQueue = make(chan *Task, Max(c.nMap, c.nReduce))

	go c.schedule()

	c.server()
	return &c
}
