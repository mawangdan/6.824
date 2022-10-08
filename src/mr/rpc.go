package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type InitReply struct {
	NReduce      int
	NMap         int
	WorkerNumber int
}

type CallForTaskReply struct {
	TaskType   int //0 for map,1 for Reduce,2 for keep call,3 for ALL DONE
	TaskNumber int //Map-X or Reduce-Y
	Filename   string
}

type DoneForTaskArgs struct {
	TaskType   int //0 for map,1 for Reduce
	TaskNumber int //Map-X or Reduce-Y
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
