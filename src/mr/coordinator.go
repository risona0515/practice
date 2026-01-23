package mr

import (
	"crypto/rand"
	"encoding/base64"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// Map任务结构体
type maptask struct {
	filepath string
	// taskStatus  status
	outfilename string
	retryCnt    int
}
type mapStruct struct {
	taskCnt                                  int
	waiting, running, retrying, done, failed []*maptask // 不要waiting了？
	mapping                                  map[string]*maptask
}

// Reduce任务结构体
type reducetask struct {
	taskid int
	// taskStatus status
	retryCnt int
}
type reduceStruct struct {
	taskCnt                                  int
	nReduce                                  int
	mediatefiles                             []string
	finalfiles                               []string
	waiting, running, retrying, done, failed []*reducetask
	mapping                                  map[string]*reducetask
}

// 记录client的结构体
type worker struct {
	id    string
	dirno int
	t     *time.Timer
	ch    chan int // 用于提前停止定时器监控协程
}
type workerStruct struct {
	// nextWorkerId int
	// mu sync.Mutex
	nextDirNo int
	workers   map[string]*worker
}

type Coordinator struct {
	// Your definitions here.
	mu           sync.Mutex
	mapTasks     mapStruct
	reduceTasks  reduceStruct
	workerStatus workerStruct
}

// type status int

// const ( // 分队列了，不需要了
// 	WAITING status = iota
// 	RUNNING
// 	RETRYING
// 	FINISHED
// )

// 退出标记
var quitting bool = false
var canquit bool = false

const MediateFileDir string = "/tmp/mrmediate"
const FinalFileDir string = "/tmp/mrfinal"

// var FinalFileDir string

const TIMEOUT = 10 * time.Second

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//	func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//		reply.Y = args.X + 1
//		return nil
//	}
func genToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(b), nil
}

func getMapTask(c *Coordinator) *maptask {
	var t *maptask
	// 先从retrying取，再从waiting取，最后从running取
	if len(c.mapTasks.retrying) != 0 {
		t = c.mapTasks.retrying[0]
		t.retryCnt++
		c.mapTasks.running = append(c.mapTasks.running, t)
		c.mapTasks.retrying = c.mapTasks.retrying[1:]
	} else if len(c.mapTasks.waiting) != 0 {
		t = c.mapTasks.waiting[0]
		c.mapTasks.running = append(c.mapTasks.running, t)
		c.mapTasks.waiting = c.mapTasks.waiting[1:]
	} else if len(c.mapTasks.running) != 0 {
		t = c.mapTasks.running[0]
		c.mapTasks.running = append(c.mapTasks.running[1:], c.mapTasks.running[0])
	} else {
		return nil
	}
	return t
}

func getReduceTask(c *Coordinator) *reducetask {
	var t *reducetask
	// 先从retrying取，再从waiting取，最后从running取
	if len(c.reduceTasks.retrying) != 0 {
		t = c.reduceTasks.retrying[0]
		t.retryCnt++
		c.reduceTasks.running = append(c.reduceTasks.running, t)
		c.reduceTasks.retrying = c.reduceTasks.retrying[1:]
	} else if len(c.reduceTasks.waiting) != 0 {
		t = c.reduceTasks.waiting[0]
		c.reduceTasks.running = append(c.reduceTasks.running, t)
		c.reduceTasks.waiting = c.reduceTasks.waiting[1:]
	} else if len(c.reduceTasks.running) != 0 {
		t = c.reduceTasks.running[0]
		c.reduceTasks.running = append(c.reduceTasks.running[1:], c.reduceTasks.running[0])
	} else {
		return nil
	}
	return t
}

// map reduce超时能否统一一下？（包括其他结构体，能否统一一下？有必要统一吗？）
func mcountdown(c *Coordinator, id string) {
	w := c.workerStatus.workers[id]
	t := w.t
	cancel := w.ch
	if t.Stop() {
		cancel <- 0 // 停止上一个任务

		// // 需要清空吗？？
		// for {
		// 	select {
		// 	case <-ch:
		// 		// 继续读取并丢弃数据
		// 	default:
		// 		// 当通道为空时，立即退出循环
		// 		break
		// 	}
		// }

	} // else { // 1.23之后不需要了？
	// 	select {
	// 		case <- t.C:
	// 		default:
	// 	}
	// }
	t.Reset(TIMEOUT)
	select {
	case <-t.C:
		// 超时处理
		c.mu.Lock()
		defer c.mu.Unlock()
		// 检查是否还在mapping中，找到任务指针
		targ, ok := c.mapTasks.mapping[id]
		if !ok {
			break
		}
		delete(c.mapTasks.mapping, id)
		// 检查是否还在running中，是的话重试次数+1移到retry，超过最大重试次数不再重试
		for idx, ta := range c.mapTasks.running {
			if ta == targ {
				l := len(c.mapTasks.running)
				c.mapTasks.running[idx] = c.mapTasks.running[l-1]
				c.mapTasks.running = c.mapTasks.running[:l-1]
				ta.retryCnt++
				if ta.retryCnt < 10 { // 重试10次，然后不再重试
					c.mapTasks.retrying = append(c.mapTasks.retrying, targ)
				} else {
					c.mapTasks.failed = append(c.mapTasks.failed, targ)
					log.Printf("maptask for %v failed after 10 retries\n", targ.filepath)
				}

			}
		}
	// 提前停止channel
	case <-cancel:
	default:
	}
}

func rcountdown(c *Coordinator, id string) {
	w := c.workerStatus.workers[id]
	t := w.t
	cancel := w.ch
	if t.Stop() {
		cancel <- 0 // 停止上一个任务
	}

	t.Reset(TIMEOUT)
	select {
	case <-t.C:
		// 超时处理
		c.mu.Lock()
		defer c.mu.Unlock()
		// 检查是否还在mapping中，找到任务指针
		targ, ok := c.reduceTasks.mapping[id]
		if !ok {
			break
		}
		delete(c.reduceTasks.mapping, id)
		// 检查是否还在running中，是的话重试次数+1移到retry，超过最大重试次数不再重试
		for idx, ta := range c.reduceTasks.running {
			if ta == targ {
				l := len(c.reduceTasks.running)
				c.reduceTasks.running[idx] = c.reduceTasks.running[l-1]
				c.reduceTasks.running = c.reduceTasks.running[:l-1]
				ta.retryCnt++
				if ta.retryCnt < 10 { // 重试10次，然后不再重试
					c.reduceTasks.retrying = append(c.reduceTasks.retrying, targ)
				} else {
					c.reduceTasks.failed = append(c.reduceTasks.failed, targ)
					log.Printf("reduce for hashid %v failed after 10 retries\n", targ.taskid)
				}
			}
		}
	// 提前停止channel
	case <-cancel:
	default:
	}
}

// 服务函数，获取一个任务
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	// 派发任务
	c.mu.Lock()
	defer c.mu.Unlock()

	clientid := args.Token

	if c.reduceTasks.taskCnt == 0 {
		// 移动最终文件到当前目录
		exePath, _ := os.Executable()
		for _, srcf := range c.reduceTasks.finalfiles {
			srcfname := filepath.Base(srcf)
			dstf := filepath.Join(exePath, srcfname)
			err := os.Rename(srcf, dstf)
			if err != nil {
				log.Printf("move file %v to exec dir failed, %v.", srcf, err)
			}
		}

		reply.IsQuit = true
		delete(c.workerStatus.workers, clientid)
		return nil
	}

	_, ok := c.workerStatus.workers[clientid]
	if !ok {
		newToken, err := genToken()
		if err != nil {
			log.Println("gen token failed ", err)
			return nil
		}
		reply.Token = newToken
		clientid = newToken
		// 更新client结构体
		w := worker{}
		w.id = newToken
		// 提供文件夹id，让worker自己创建
		w.dirno = c.workerStatus.nextDirNo
		c.workerStatus.nextDirNo++
		w.t = time.NewTimer(time.Hour) // 防止立即触发
		w.t.Stop()
		w.ch = make(chan int)
		c.workerStatus.workers[newToken] = &w
	}

	if c.mapTasks.taskCnt != 0 { // 顺序 retry -> waiting -> running
		reply.Type = MAPTASK
		reply.Dstdir = filepath.Join(MediateFileDir, strconv.Itoa(c.workerStatus.workers[clientid].dirno))
		var mt *maptask
		mt = getMapTask(c)
		if mt == nil {
			log.Println("get map task failed, no tasks left but tried to get one")
			return nil
		}
		c.mapTasks.mapping[clientid] = mt
		reply.Filepath = mt.filepath
		go mcountdown(c, clientid)
	} else if c.reduceTasks.taskCnt != 0 {
		reply.Type = REDUCETASK
		// reply.Dstdir = FinalFileDir
		reply.Dstdir = filepath.Join(FinalFileDir, strconv.Itoa(c.workerStatus.workers[clientid].dirno)) // 先也存到中间文件夹，report done中拷贝到最终文件夹
		var rt *reducetask
		rt = getReduceTask(c)
		if rt == nil {
			log.Println("get reduce task failed, no tasks left but tried to get one")
			return nil
		}
		c.reduceTasks.mapping[clientid] = rt
		reply.MediateFiles = c.reduceTasks.mediatefiles
		reply.Reduceid = rt.taskid
		reply.NReduce = c.reduceTasks.nReduce
		go rcountdown(c, clientid)
	}

	return nil
}

// 服务函数，通知任务完成
func (c *Coordinator) ReportTaskDone(args *ReportTaskArgs, reply *int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	id := args.Token
	_, ok := c.workerStatus.workers[id]
	if !ok {
		return nil
	}
	if args.Type == MAPTASK {
		targ, ok := c.mapTasks.mapping[id]
		if !ok {
			return nil
		}
		delete(c.mapTasks.mapping, id)
		for idx, ta := range c.mapTasks.running {
			if ta == targ {
				// 从running队列移到done队列
				l := len(c.mapTasks.running)
				c.mapTasks.running[idx] = c.mapTasks.running[l-1]
				c.mapTasks.running = c.mapTasks.running[:l-1]
				c.mapTasks.done = append(c.mapTasks.done, targ)
				// 任务计数-1
				c.mapTasks.taskCnt--
				// 填写中间文件位置
				targ.outfilename = args.Outfile
				// 把中间文件放到reduce列表中
				c.reduceTasks.mediatefiles = append(c.reduceTasks.mediatefiles, args.Outfile)
			}
		}
	} else if args.Type == REDUCETASK {
		targ, ok := c.reduceTasks.mapping[id]
		if !ok {
			return nil
		}
		delete(c.reduceTasks.mapping, id)
		for idx, ta := range c.reduceTasks.running {
			if ta == targ {
				// 从running队列移到done队列
				l := len(c.reduceTasks.running)
				c.reduceTasks.running[idx] = c.reduceTasks.running[l-1]
				c.reduceTasks.running = c.reduceTasks.running[:l-1]
				c.reduceTasks.done = append(c.reduceTasks.done, targ)
				// 任务计数-1
				c.reduceTasks.taskCnt--
				// 记录最终文件
				c.reduceTasks.finalfiles = append(c.reduceTasks.finalfiles, args.Outfile)
			}
		}
	}
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
	// ret := false

	// Your code here.
	if c.reduceTasks.taskCnt != 0 {
		return false
	}

	if !quitting {
		go func() {
			log.Printf("preparing to quit, waiting for workers to end %v\n", time.Now())
			quitting = true
			t := time.NewTimer(2 * TIMEOUT)
			<-t.C
			log.Printf("quit at %v\n", time.Now())
			canquit = true
		}()
	}
	return canquit

	// return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mapTasks.mapping = make(map[string]*maptask)
	c.reduceTasks.mapping = make(map[string]*reducetask)
	c.workerStatus.workers = make(map[string]*worker)

	// Your code here.
	// create path
	// 中间文件目录，只创建外层目录，每个worker的文件夹worker自己创建
	err := os.RemoveAll(MediateFileDir)
	if err != nil {
		return nil
	}
	err = os.MkdirAll(MediateFileDir, 0766)
	if err != nil {
		log.Println("create dir error ", err)
	}
	// 最终文件目录
	err = os.RemoveAll(FinalFileDir)
	if err != nil {
		return nil
	}
	err = os.MkdirAll(FinalFileDir, 0766)
	if err != nil {
		log.Println("create dir error ", err)
	}

	// initialize map reduce tasks
	exePath, err := os.Executable()
	// FinalFileDir = exePath
	exeDir := filepath.Dir(exePath)
	for _, f := range files {
		c.mapTasks.taskCnt++
		c.mapTasks.waiting = append(c.mapTasks.waiting,
			&maptask{filepath: filepath.Join(exeDir, f)})
	}

	c.reduceTasks.taskCnt = nReduce
	c.reduceTasks.nReduce = nReduce
	c.reduceTasks.waiting = make([]*reducetask, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks.waiting[i] = &reducetask{taskid: i, retryCnt: 0}
	}

	// initialize timer

	c.server()
	return &c
}
