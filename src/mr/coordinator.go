package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

//task 状态
const (
	idle       int = 0
	inProgress int = 1
	completed  int = 2
)

type MapTask struct {
	state int //0 idle,1 in-progress,2 completed
}

type ReduceTask struct {
	state int //0 idle,1 in-progress,2 completed
}

type Coordinator struct {
	// Your definitions here.
	nReduce       int
	nMap          int
	reduceDoneNum int
	mapDoneNum    int
	mapTask       []MapTask
	reduceTask    []ReduceTask
	mapLock       sync.Mutex
	reduceLock    sync.Mutex
	files         []string
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

//初始化worker 的reduce数量
func (c *Coordinator) initCall(args *ExampleArgs, reply *InitReply) error {
	reply.nReduce = c.nReduce
	reply.nMap = c.nMap
	return nil
}

//请求task
func (c *Coordinator) callForTask(args *ExampleArgs, reply *CallForTaskReply) error {
	mapNum := c.atomicMap()
	if mapNum != -1 { //分配maptask成功
		reply.taskType = 0
		reply.taskNumber = mapNum
		reply.filename = c.files[mapNum]
		return nil
	}

	//所有的map已经完成,reduce还没完成
	if c.mapDoneNum == c.nMap && c.reduceDoneNum < c.nReduce {
		//请求reduce
		reduceNum := c.atomicReduce()
		if reduceNum != -1 { //分配reducetask成功
			reply.taskType = 1
			reply.taskNumber = reduceNum
		}
	} else if c.reduceDoneNum == c.nReduce { //reduce全部做完
		reply.taskType = 3
	} else {
		reply.taskType = 2 //保持请求
	}
	return nil
}

//task完成
func (c *Coordinator) taskDone(args *DoneForTaskArgs, reply *ExampleReply) error {
	//map done
	if args.taskType == 0 {
		c.mapTask[args.taskNumber].state = completed
		c.mapLock.Lock()
		c.mapDoneNum++
		c.mapLock.Unlock()
	} else if args.taskType == 1 { //reduce done
		c.reduceTask[args.taskNumber].state = completed
		c.reduceLock.Lock()
		c.reduceDoneNum++
		c.reduceLock.Unlock()
	}
	return nil
}

//返回文件的编号,如果返回-1则map全部开始
func (c *Coordinator) atomicMap() int {
	tmp := -1
	c.mapLock.Lock()
	for i := 0; i < len(c.mapTask); i++ {
		if c.mapTask[i].state == idle {
			c.mapTask[i].state = inProgress
			tmp = i
			break
		}
	}
	c.mapLock.Unlock()
	return tmp
}

//返回reduce的编号,如果返回-1则reduce全部开始
func (c *Coordinator) atomicReduce() int {
	tmp := -1
	c.reduceLock.Lock()
	for i := 0; i < len(c.reduceTask); i++ {
		if c.reduceTask[i].state == idle {
			c.reduceTask[i].state = inProgress
			tmp = i
			break
		}
	}
	c.reduceLock.Unlock()
	return tmp
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
	return c.nReduce == c.reduceDoneNum
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here. init
	for i := 0; i < len(files); i++ {
		c.mapTask = append(c.mapTask, MapTask{idle})
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTask = append(c.reduceTask, ReduceTask{idle})
	}
	c.reduceDoneNum = 0
	c.mapDoneNum = 0
	c.nReduce = nReduce
	c.files = files
	c.nMap = len(files)
	c.server()
	return &c
}
