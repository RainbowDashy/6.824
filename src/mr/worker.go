package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "sort"
import "os"
import "encoding/json"
import "io/ioutil"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

var nReduce int

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func hash(key string) int {
	return ihash(key) % nReduce
}

type ByHashKey []KeyValue

func (a ByHashKey) Len() int           { return len(a) }
func (a ByHashKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByHashKey) Less(i, j int) bool { return hash(a[i].Key) < hash(a[j].Key) }

type ByKey []KeyValue

func (a ByKey) Len() int {return len(a)}
func (a ByKey) Swap(i, j int) {a[i], a[j] = a[j], a[i]}
func (a ByKey) Less(i, j int) bool {return a[i].Key < a[j].Key}

func store(kv []KeyValue, m int) {
	sort.Sort(ByHashKey(kv))

	i := 0
	for i < len(kv) {
		j := i + 1
		for j < len(kv) && hash(kv[j].Key) == hash(kv[i].Key) {
			j++
		}
		r := hash(kv[i].Key)
		name := fmt.Sprintf("mr-%v-%v", m, r)
		tmpfile, _ := ioutil.TempFile("", "")
		enc := json.NewEncoder(tmpfile)
		for k := i; k < j; k++ {
			enc.Encode(&kv[k])
		}
		i = j
		tmpfile.Close()
		os.Rename(tmpfile.Name(), name)
	}
}

func do_map(mapf func(string, string) []KeyValue, reply *JobReply) {
	nReduce = reply.NReduce
	filename := reply.Filename
	id := reply.Id
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
	store(kva, id)
	report := JobReport{1, id}
	call("Master.JReport", &report, reply)
}

func do_reduce(reducef func(string, []string) string, reply *JobReply) {
	nReduce = reply.NReduce
	id := reply.Id
	kva := []KeyValue{}
	for m := 0; m < reply.NMap; m++ {
		filename := fmt.Sprintf("mr-%v-%v", m, id)
		file, err := os.Open(filename)
		if err != nil {
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}


	sort.Sort(ByKey(kva))
	name := fmt.Sprintf("mr-out-%v", id)
	tmpfile, _ := ioutil.TempFile("", "")

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	tmpfile.Close()
	os.Rename(tmpfile.Name(), name)
	report := JobReport{2, id}
	call("Master.JReport", &report, reply)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		args := JobRequest{}
		reply := JobReply{}
		call("Master.JAlloc", &args, &reply)
		switch reply.Job {
		case 0:
			time.Sleep(time.Second)
		case 1:
			do_map(mapf, &reply)
		case 2:
			do_reduce(reducef, &reply)
		}
	}
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
