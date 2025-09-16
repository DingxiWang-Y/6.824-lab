package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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

	// Your worker implementation here.
	//通过for循环不断向Master请求任务
	for{
		args := TaskArgs{}
		reply :=TaskReply{}

		ok := call("master.AssignTask",&args, &reply)

		if !ok{
			fmt.Println("RPC Call has FIled,Exit")
			break
		}

		switch reply.TaskType{
		case MapTask:
			fmt.Printf("Worker: received Map task #%d on file %s\n", reply.TaskID, reply.MapInputFile)
			handleMapTask(mapf, reply)
		case ReduceTask:
			// TODO: 在后续步骤中实现 Reduce 任务的处理
			fmt.Printf("Worker: received Reduce task #%d\n", reply.TaskID)
		case WaitTask:
			// Master 暂时没有任务可分配，等待一会再重试
			fmt.Println("Worker: received Wait task, sleeping.")
			time.Sleep(1 * time.Second)
		case EixtTask:
			// Master 发出退出指令
			fmt.Println("Worker: received Exit task. Exiting.")
			return
		default:
			fmt.Printf("Worker: received unknown task type: %v\n", reply.TaskType)
		}
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}


func handleMapTask(mapf func(string,string)[]KeyValue, task TaskReply){
	filename := task.MapInputFile
	file, err := os.Open(filename)
	if err != nil{
		log.Fatalf("Can't open file %v",filename)
		return
	}
	content, err := io.ReadAll(file)
	if err != nil{
		log.Fatal("Can't read filename %v", filename)
		file.Close()
		return
	}
	file.Close()

	kva := mapf(filename,string(content))

	nReduce := task.NReduce
	intermediateFiles := make([][] KeyValue,nReduce)
	for i:=0; i<nReduce; i++{
		intermediateFiles[i] = []KeyValue{}
	}

	//对数据进行分区，分为一个二维数组，将同一个文件的数据存储在同一行中
	for _, kv := range(kva){
		reduceTaksNumber := ihash(kv.Key)%nReduce
		intermediateFiles[reduceTaksNumber] = append(intermediateFiles[reduceTaksNumber], kv)
	}

	//将中间数据写入文件
	for i:=0; i<nReduce; i++{
		//mr-x-y,X是map的任务ID，Y是reduce的任务号
		intermediateFileNames := fmt.Sprintf("mr-%d-%d",task.TaskID,i)
		file,err := os.Create(intermediateFileNames)
		if err != nil{
			log.Fatalf("Can't create %v",intermediateFileNames)
			return
		}
		enc := json.NewEncoder(file)  
		for _,kv := range(intermediateFiles[i]){
			err := enc.Encode(&kv)
			if err != nil{
				log.Fatalf("Con't write to %v",intermediateFileNames)
			}
		}
		file.Close()
	}


}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

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
