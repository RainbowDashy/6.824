package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "fmt"

type Master struct {
	nMap, nReduce int
	files         []string
	maptask       []int8 // 0-idle 1-in-progress 2-done
	reducetask    []int8
	mapdone       bool
	lock          sync.Mutex
	done	bool
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) waitmap(i int) {
	fmt.Println("Map ", i)
	time.Sleep(10 * time.Second)
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.maptask[i] == 1 {
		m.maptask[i] = 0
	}
}

func (m *Master) waitreduce(i int) {
	fmt.Println("Reduce ", i)
	time.Sleep(20 * time.Second)
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.reducetask[i] == 1 {
		m.reducetask[i] = 0
	}
}

func (m *Master) JAlloc(args *JobRequest, reply *JobReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	reply.NReduce = m.nReduce
	reply.NMap = m.nMap
	for i, v := range m.maptask {
		if v == 0 {
			m.maptask[i] = 1
			reply.Job = 1
			reply.Filename = m.files[i]
			reply.Id = i
			go m.waitmap(i)
			return nil
		}
	}
	if !m.mapdone {
		for _, v := range m.maptask {
			if v != 2 {
				return nil
			}
		}
		m.mapdone = true
		fmt.Println("Map Done!")
	}
	for i, v := range m.reducetask {
		if v == 0 {
			m.reducetask[i] = 1
			reply.Job = 2
			reply.Id = i
			go m.waitreduce(i)
			return nil
		}
	}
	m.done = true
	return nil
}

func (m *Master) JReport(args *JobReport, reply *JobReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	switch args.Job {
	case 1:
		m.maptask[args.Id] = 2
	case 2:
		m.reducetask[args.Id] = 2
	}
	return nil
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := m.done
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.nReduce = nReduce
	m.nMap = len(files)
	m.files = files
	m.maptask = make([]int8, m.nMap)
	m.reducetask = make([]int8, nReduce)
	m.server()
	return &m
}
