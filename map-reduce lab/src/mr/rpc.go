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

// Add your RPC definitions here.
type TaskType int
const (
	MapTask TaskType = iota //map task
	ReduceTask							//reduce task
	WaitTask								//需要等待任务
	EixtTask								//推出任务
)


type TaskArgs struct{		//woker 返回给master的类型，可以为空类型
}

type TaskReply struct{
	//通用字段
	TaskType TaskType	//区分任务类型
	TaskID int //唯一的任务号

	//map任务相关字段
	MapInputFile string
	NReduce int //此处是map的数据定义，需要提前知道有多少个Reduce任务，之后才知道将中间值交给谁
	//如果使用hash代码，键%Nreduce -> 得到最终分配的机器是那一台
	
	//reduce 任务相关字段
	NMap int //NMap是用在Reduce机器中的
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
