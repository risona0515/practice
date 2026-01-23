package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	var id string

	for true {
		request := GetTaskArgs{Token: id}
		reply := GetTaskReply{Token: id, IsQuit: false}
		getJob(&request, &reply)
		if reply.IsQuit {
			break
		}
		if id != reply.Token {
			id = reply.Token
		}

		// 检查目录是否存在，不存在则创建
		dstdir := reply.Dstdir
		os.MkdirAll(dstdir, os.ModePerm)
		// info, err := os.Stat(dstdir)
		// if err != nil {
		// 	log.Println(err)
		// 	return false
		// }
		// if !info.IsDir() {
		// 	log.Printf("destination %v not a folder\n", dstdir)
		// }

		tasktype := reply.Type
		var midfile string
		if tasktype == MAPTASK {
			retok := procMapWork(&reply, mapf, &midfile)
			if !retok {
				reply.Token = "" // 失败场景暂时用无token来表示
			}
		} else if tasktype == REDUCETASK {
			retok := procReduceWork(&reply, reducef, &midfile)
			if !retok {
				reply.Token = ""
			}
		}
		reportargs := ReportTaskArgs{Type: tasktype, Token: id, Outfile: midfile}
		reportDone(&reportargs, nil)
	}
	log.Printf("worker inner exit %v\n", id)
}

// 返回值，成功 true，失败 false
func procMapWork(reply *GetTaskReply, mapf func(string, string) []KeyValue, outfile *string) bool {
	dstdir := reply.Dstdir
	fpath := reply.Filepath

	// 调用map函数做统计
	intermediate := []KeyValue{}
	f, err := os.Open(fpath)
	if err != nil {
		log.Println("cannot open", fpath)
		log.Println(err)
	}
	content, err := io.ReadAll(f)
	if err != nil {
		log.Println("cannot read", fpath)
		log.Println(err)
	}
	f.Close()
	kva := mapf(fpath, string(content))
	intermediate = append(intermediate, kva...)

	// 拼接写入的中间文件路径
	fname := filepath.Base(fpath)
	ext := filepath.Ext(fname)
	fnameNoExt := strings.TrimSuffix(fname, ext)

	// 创建写入的中间文件路径
	outpath := filepath.Join(dstdir, fnameNoExt+".json")
	f, err = os.Create(outpath)
	if err != nil { // 这里检查过了，前面是否可以不用检查目录是否创建？
		log.Println(err)
		return false
	}
	defer f.Close()
	*outfile = outpath

	// debug 打印看看
	// fmt.Println(intermediate)

	// 写入map
	encoder := json.NewEncoder(f)
	encoder.Encode(intermediate)

	return true
}

// 返回值，成功 true，失败 false
func procReduceWork(reply *GetTaskReply, reducef func(string, []string) string, outfile *string) bool {
	dstdir := reply.Dstdir
	files := reply.MediateFiles
	reduceid := reply.Reduceid
	nreduce := reply.NReduce

	// 先创建输出文件
	oname := fmt.Sprintf("mr-out-%d", reduceid)
	outpath := filepath.Join(dstdir, oname)
	ofile, err := os.Create(outpath)
	if err != nil {
		log.Println(err)
		return false
	}
	defer ofile.Close()
	*outfile = outpath

	// output := make(map[string]int)
	gather := map[string][]string{}

	for _, midfile := range files {
		f, err := os.Open(midfile)
		if err != nil {
			log.Println(err)
			return false
		}
		intermediate := []KeyValue{}
		decoder := json.NewDecoder(f)
		err = decoder.Decode(&intermediate)
		if err != nil {
			log.Println(err)
			f.Close()
			return false
		}
		f.Close()

		i := 0 // 可以直接在for后面定义吗
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			if ihash(intermediate[i].Key)%nreduce == reduceid {
				for k := i; k < j; j++ {
					gather[intermediate[i].Key] = append(gather[intermediate[i].Key], intermediate[k].Value)
				}
				// count := reducef(intermediate[i].Key, values)
				// output[intermediate[k].Key] += count
				//fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
			}
			i = j
		}
	}
	for k, v := range gather {
		output := reducef(k, v)
		fmt.Fprintf(ofile, "%v %v\n", k, output)
	}
	return true
}

func getJob(args *GetTaskArgs, reply *GetTaskReply) {
	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetTask", args, reply)
	if ok {
		// reply.Y should be 100.
		log.Printf("worker %v processing %v %v\n", reply.Token, reply.Filepath, reply.Reduceid)
	} else {
		log.Printf("%v get work failed!\n", reply.Token)
	}
}

func reportDone(args *ReportTaskArgs, reply *int) {
	ok := call("Coordinator.ReportTaskDone", args, nil)
	if ok {
		// reply.Y should be 100.
		log.Printf("worker %v report done\n", args.Token)
	} else {
		log.Printf("%v report done failed!\n", args.Token)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
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
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		log.Printlnf("reply.Y %v\n", reply.Y)
// 	} else {
// 		log.Printlnf("call failed!\n")
// 	}
// }

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

	log.Println(err)
	return false
}
