/********* server ***********/

package main

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
)

// 定义参数结构体
type Args struct {
	A, B int
}

// 定义服务对象
type Arith struct{}

// 定义 RPC 方法：必须是导出的，且符合 (args, *reply) 的模式
func (t *Arith) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

func main() {
	arith := new(Arith)
	// 1. 注册 RPC 服务
	rpc.Register(arith)
	// 2. 将 RPC 挂载到 HTTP 路由（net/rpc 提供的便捷方式）
	rpc.HandleHTTP()

	// 3. 监听端口
	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Println("Server 正在监听端口 1234...")
	http.Serve(l, nil)
}



/********* client ***********/

package main

import (
	"fmt"
	"log"
	"net/rpc"
)

type Args struct {
	A, B int
}

func main() {
	// 1. 连接远程 RPC 服务（假设 Server 在同一台机器或指定 IP）
	client, err := rpc.DialHTTP("tcp", "127.0.0.1:1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	// 2. 准备请求参数
	args := &Args{7, 8}
	var reply int

	// 3. 同步调用远程方法 "Arith.Multiply"
	err = client.Call("Arith.Multiply", args, &reply)
	if err != nil {
		log.Fatal("arith error:", err)
	}

	fmt.Printf("RPC 结果: %d * %d = %d\n", args.A, args.B, reply)
}