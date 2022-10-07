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

type CallForJobArgs struct {
	args int
}

type CallForTaskReply struct {
	taskType   int //0 for map,1 for Reduce,2 for Map DONE,3 for ALL DONE
	taskNumber int //Map-X or Reduce-Y
}

type DoneForTaskArgs struct {
	taskType   int //0 for map,1 for Reduce
	taskNumber int //Map-X or Reduce-Y
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
