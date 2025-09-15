package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type TaskStatus int
const(
	Idea TaskStatus = iota
	InProgress
	Completed
)

type Task struct{
	ID int
	Status TaskStatus
	TaskType TaskType
	FileNmae string //只在map阶段会使用到
}


type Master struct {
	// Your definitions here.
	mapTasks [] Task	//所有map任务
	reduceTask [] Task	//所有reduce任务

	nMap int
	nReduce int

}

// Your code here -- RPC handlers for the worker to call.
//如果有Worker来请求map/reduce任务，则通过遍历任务指派相应的map/reduce任务给相应的worker
func (m *Master) AssignTask(args *TaskArgs,reply *TaskReply) error{
	//ToDo:在后续的操作中需要加锁来限制并发

	for i:= range m.mapTasks{
			if m.mapTasks[i].Status == Idea{
				//找到一个空闲的map任务,分配给相应的Worker
				m.mapTasks[i].Status = InProgress
				//ToDo 在后续需要启动一个计时器，来处理任务超时


				reply.TaskType = MapTask
				reply.TaskID = m.mapTasks[i].ID
				reply.NReduce = m.nReduce
				return nil
			}
	}
			//ToDo如果Map任务全都完成，则分配reduce任务，后期需要加一个判断函数


			//如果所有Map都完成，则告诉Worker等待
			reply.TaskType = WaitTask
			return nil

}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
/***
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
	***/


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
	ret := false

	// Your code here.
	// ToDo 在后续的阶段中需要判断所有的任务是否都完成


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nReduce = nReduce
	m.nMap = len(files)
	
	//进行初始化map task
	m.mapTasks = make([]Task, m.nMap)
	for i,file := range(files){
		m.mapTasks[i] = Task{
			ID: i,
			TaskType: MapTask,
			Status: Idea,
			FileNmae: file,
		}
	}

	//进行初始化reduce task
	m.reduceTask = make([]Task, m.nReduce)
	for i:=0; i < nReduce; i++{
		m.reduceTask[i] = Task{
			ID: i,
			TaskType: ReduceTask,
			Status: Idea,
		}
	}

	m.server()
	return &m
}
