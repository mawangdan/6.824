package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	nReduce       int
	isReduce      []bool
	isReduceDone  []bool
	reduceDoneNum int
	mapDoneNum    int
	files         []string
	isMap         []bool
	isMapDone     []bool
	requestLock   sync.Mutex
	doneLock      sync.Mutex
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

//请求task
func (c *Coordinator) callForTask(args *ExampleArgs, reply *CallForTaskReply) error {
	mapNum := c.atomicMap()
	if mapNum != -1 {
		reply.taskType = 0
		reply.taskNumber = mapNum
		return nil
	}

	//所有的map已经完成
	if c.mapDoneNum == len(c.files) {
		//请求reduce
		reduceNum := c.atomicReduce()
		if reduceNum != -1 {
			reply.taskType = 1
			reply.taskNumber = reduceNum
			return nil
		}
	}

	if c.reduceDoneNum == c.nReduce { //reduce全部做完
		reply.taskType = 3
		return nil
	} else {
		reply.taskType = 2 //reduce还没做完，reduce全都开始做了
		return nil
	}
}

//task完成
func (c *Coordinator) taskDone(args *DoneForTaskArgs, reply *CallForTaskReply) error {
	//map done
	if args.taskType == 0 {
		c.isMapDone[args.taskNumber] = true
		c.doneLock.Lock()
		c.mapDoneNum++
		c.doneLock.Unlock()
	} else if args.taskType == 1 { //reduce done
		c.isReduceDone[args.taskNumber] = true
		c.doneLock.Lock()
		c.reduceDoneNum++
		c.doneLock.Unlock()
	}
	return nil
}

//返回文件的编号,如果返回-1则map全部开始
func (c *Coordinator) atomicMap() int {
	tmp := -1
	c.requestLock.Lock()
	for i := 0; i < len(c.isMap); i++ {
		if !c.isMap[i] {
			c.isMap[i] = true
			tmp = i
			break
		}
	}
	c.requestLock.Unlock()
	return tmp
}

//返回reduce的编号,如果返回-1则reduce全部开始
func (c *Coordinator) atomicReduce() int {
	tmp := -1
	c.requestLock.Lock()
	for i := 0; i < len(c.isReduce); i++ {
		if !c.isReduce[i] {
			c.isReduce[i] = true
			tmp = i
			break
		}
	}
	c.requestLock.Unlock()
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
		c.isMapDone = append(c.isMapDone, false)
		c.isMap = append(c.isMap, false)
	}
	for i := 0; i < nReduce; i++ {
		c.isReduceDone = append(c.isReduceDone, false)
		c.isReduce = append(c.isReduce, false)
	}
	c.reduceDoneNum = 0
	c.mapDoneNum = 0
	c.nReduce = nReduce
	c.files = files

	c.server()
	return &c
}
