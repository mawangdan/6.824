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

var nReduce int
var nMap int

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type KeyValues []KeyValue

func (s KeyValues) Len() int {
	return len(s)
}

func (s KeyValues) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s KeyValues) Less(i, j int) bool {
	return s[i].Key < s[j].Key
}

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

	initReply := initCall()
	nMap = initReply.NMap
	nReduce = initReply.NReduce
	// Your worker implementation here.
	for true {
		log.Printf("loop start")
		// send the Example RPC to the coordinator.
		reply := workerCallForTask()
		if reply.TaskType == 0 {
			//do map
			workerMap(mapf, reply.Filename, reply.TaskNumber)
			//finish
			callTaskDone(reply.TaskType, reply.TaskNumber)
		} else if reply.TaskType == 1 {
			//do reduce
			workerReduce(reducef, reply.TaskNumber)
			//finish
			callTaskDone(reply.TaskType, reply.TaskNumber)
		} else if reply.TaskType == 3 { //全部完成了
			break
		} else {
			//其他状态继续不断请求task
			time.Sleep(time.Second)
		}
	}
}

//获得nReduce,nMap
func initCall() InitReply {
	args := ExampleArgs{}
	reply := InitReply{}
	ok := call("Coordinator.InitCall", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
	log.Printf("init finish nmap %d nreduce %d", reply.NMap, reply.NReduce)
	return reply
}

//请求一个task
func workerCallForTask() CallForTaskReply {
	args := ExampleArgs{}
	reply := CallForTaskReply{}
	ok := call("Coordinator.CallForTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
	log.Printf("get task type%d num %d filename %s", reply.TaskType, reply.TaskNumber, reply.Filename)
	return reply
}

//请求结束
func callTaskDone(taskType int, taskNumber int) {
	args := DoneForTaskArgs{}
	args.TaskType = taskType
	args.TaskNumber = taskNumber
	reply := ExampleReply{}
	ok := call("Coordinator.TaskDone", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
	log.Printf("task done type %d num %d", taskType, taskNumber)
}

//执行map
func workerMap(mapf func(string, string) []KeyValue, filename string, taskNumber int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kvs := mapf(filename, string(content))
	reduceKvs := [][]KeyValue{}
	for i := 0; i < nReduce; i++ {
		reduceKvs = append(reduceKvs, []KeyValue{})
	}
	//每个kv丢到合适的reduce桶
	for _, v := range kvs {
		reduceY := ihash(v.Key) % nReduce
		reduceKvs[reduceY] = append(reduceKvs[reduceY], KeyValue{v.Key, v.Value})
	}
	//reduceKvs[Y]输出为mr-X-Y
	for i := 0; i < nReduce; i++ {
		sort.Sort(KeyValues(reduceKvs[i]))

		jsonFilename := fmt.Sprintf("mr-%d-%d.json", taskNumber, i)
		// 创建文件
		filePtr, err := os.Create(jsonFilename)
		if err != nil {
			fmt.Println("文件创建失败", err.Error())
			return
		}
		enc := json.NewEncoder(filePtr)
		for _, kv := range reduceKvs[i] {
			enc.Encode(&kv)
		}
		filePtr.Close()
	}
}

//执行reduce
func workerReduce(reducef func(string, []string) string, taskNumber int) {
	kva := []KeyValue{}
	for i := 0; i < nMap; i++ {
		jsonFilename := fmt.Sprintf("mr-%d-%d.json", i, taskNumber)

		file, err := os.Open(jsonFilename)
		if err != nil {
			fmt.Println("json文件读取失败", err.Error())
			return
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

	//归并key
	//这里其实可以套用归并排序,如果循环中组合成map再reduce的策略,
	//但是空间复杂度可能远大于O(K+V),如果循环中进行reduce空间复杂度是O(V),双指针
	sort.Sort(KeyValues(kva))
	var reduceResult KeyValues
	for i := 0; i < len(kva); {
		j := i
		var sa []string
		for ; j < len(kva) && kva[i].Key == kva[j].Key; j++ {
			sa = append(sa, kva[j].Value)
		}
		s := reducef(kva[i].Key, sa)
		reduceResult = append(reduceResult, KeyValue{kva[i].Key, s})
		i = j
	}

	//写入mr-out-Y
	outFilename := fmt.Sprintf("mr-out-%d", taskNumber)
	ofile, _ := os.Create(outFilename)
	for _, v := range reduceResult {
		fmt.Fprintf(ofile, "%v %v\n", v.Key, v.Value)
	}
	ofile.Close()
	//写完后删除中间文件

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
