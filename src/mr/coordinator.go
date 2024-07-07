package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var mu sync.Mutex

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int   // reducer的个数
	TaskId            int   // 生成task的id, 自增
	Phase             Phase // 框架所处于的阶段
	TaskChannelMap    chan *Task
	TaskChannelReduce chan *Task
	TaskMap           map[int]*Task
	files             []string // 传入的文件数组
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch c.Phase {
	case MapPhase:
		{
			if len(c.TaskChannelMap) > 0 {
				*reply = *<-c.TaskChannelMap
				if !c.judgeState(reply.TaskId) { // 不处于waitting, 此时状态变为working
					fmt.Printf("[DEBUG] MapTask-id[%d] is working\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaittingTask // map任务分发完毕, 让worker等待
				if c.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case ReducePhase:
		{
			if len(c.TaskChannelReduce) > 0 {
				*reply = *<-c.TaskChannelReduce
				if !c.judgeState(reply.TaskId) { // 不处于waitting, 此时状态变为working
					fmt.Printf("[DEBUG] ReduceTask-id[%d] is working\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaittingTask // map任务分发完毕, 让worker等待
				if c.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case AllDone:
		{
			reply.TaskType = ExitTask
		}
	default:
		{
			panic("[ERROR] the phase undefined")
		}
	}
	return nil
}

func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.TaskType {
	case MapTask:
		task, ok := c.TaskMap[args.TaskId]
		if ok && task.State == Working {
			task.State = Done
		} else {
			fmt.Printf("[DEBUG] Map task Id[%d] is already finished\n", args.TaskId)
		}
	case ReduceTask:
		{
			task, ok := c.TaskMap[args.TaskId]
			if ok && task.State == Working {
				task.State = Done
			} else {
				fmt.Printf("[DEBUG] Map task Id[%d] is already finished\n", args.TaskId)
			}
		}
	default:
		panic("[ERROR] The task type undefined")
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	if c.Phase == AllDone {
		fmt.Println("[INFO] All tasks are finished, the coordinator will be exit!")
		return true
	} else {
		return false
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		ReducerNum:        nReduce,
		Phase:             MapPhase,
		TaskChannelMap:    make(chan *Task, len(files)),
		TaskChannelReduce: make(chan *Task, nReduce),
		TaskMap:           make(map[int]*Task, len(files)+nReduce),
	}

	// Your code here.
	c.makeMapTasks(files)
	c.server()
	go c.CrashDetector()
	return &c
}

// 生产map任务
func (c *Coordinator) makeMapTasks(files []string) {
	for _, v := range files {
		id := c.generateTaskId()
		task := Task{
			TaskType:   MapTask,
			TaskId:     id,
			ReducerNum: c.ReducerNum,
			FileSlice:  []string{v},
			State:      Waiting,
		}
		c.TaskMap[id] = &task
		c.TaskChannelMap <- &task
	}
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateTaskId()
		task := Task{
			TaskId:     id,
			TaskType:   ReduceTask,
			FileSlice:  selectReduceName(i),
			State:      Waiting,
			ReducerNum: c.ReducerNum,
		}
		c.TaskMap[id] = &task
		c.TaskChannelReduce <- &task
	}
}

func (c *Coordinator) generateTaskId() int {
	res := c.TaskId
	c.TaskId++
	return res
}

func (c *Coordinator) judgeState(taskId int) bool {
	task, ok := c.TaskMap[taskId]
	if !ok || task.State != Waiting {
		return false
	}
	task.StartTime = time.Now()
	task.State = Working
	return true
}

func (c *Coordinator) checkTaskDone() bool {
	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)
	for _, v := range c.TaskMap {
		if v.TaskType == MapTask {
			if v.State == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.TaskType == ReduceTask {
			if v.State == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}
	// 如果map或者reduce全做完了，需要进入下一阶段，return true
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		fmt.Println("[DEBUG] Map tasks all done! To reduce phase!")
		return true
	} else if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
		fmt.Println("[DEBUG] Reduce tasks all done! To alldone phase")
		return true
	}
	return false
}

func (c *Coordinator) toNextPhase() {
	if c.Phase == MapPhase {
		c.TaskId = 0
		c.makeReduceTasks()
		c.Phase = ReducePhase
	} else if c.Phase == ReducePhase {
		c.Phase = AllDone
	}
}

// 读取文件列表，选择Map阶段生成的tmp文件
func selectReduceName(reduceNum int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, fi.Name())
		}
	}
	return s
}

func (c *Coordinator) CrashDetector() {
	for {
		// 2s检测一次
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.Phase == AllDone {
			mu.Unlock()
			break
		}
		for _, task := range c.TaskMap {
			if task.State == Working && time.Since(task.StartTime) > 10*time.Second {
				fmt.Println("[INFO] the task [", task.TaskId,
					"] is crash, take ", time.Since(task.StartTime).Seconds(), " s")
				switch task.TaskType {
				case MapTask:
					task.State = Waiting
					c.TaskChannelMap <- task
				case ReduceTask:
					task.State = Waiting
					c.TaskChannelReduce <- task
				}
			}
		}
		mu.Unlock()
	}
}
