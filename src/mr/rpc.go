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

// Add your RPC definitions here.

// Job: 0-request job 1-map done 2-reduce done
type JobRequest struct {
	Filename string
	Id       int
}

// 0-idle 1-map 2-reduce
type JobReply struct {
	Job      int
	Filename string
	Id       int
	NReduce  int
	NMap	int
}

type JobReport struct {
	Job int
	Id int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
