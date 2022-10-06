package main

import (
	"context"
	"flag"
	"fmt"
	. "gitee.com/knullhhf/hack22/logger"
	"gitee.com/knullhhf/hack22/net/msg"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"sync"
	"time"
)

type CtlCli struct {
	addr    string
	info    msg.ClientInfo
	cli     msg.ToolsManagerClient
	taskSrv taskService

	ctx    context.Context
	cancel context.CancelFunc
	//
	wg sync.WaitGroup
}

type cliTask struct {
	name     string
	con      net.Conn
	info     msg.TaskInfo
	state    msg.TaskState
	progress string
}

func (cc *CtlCli) init4Task(taskAddr string) {
	cc.taskSrv.taskAddr = taskAddr
	cc.taskSrv.ctx = cc.ctx
	cc.taskSrv.wg = &cc.wg
	cc.taskSrv.tasks = map[string]*cliTask{}
	gs := grpc.NewServer()
	msg.RegisterTaskManagerServer(gs, &cc.taskSrv)
	li, err := net.Listen("tcp", taskAddr)
	if err != nil {
		LogFatal("listen taskAddr(%s) err:%s", taskAddr, err.Error())
	}
	gs.Serve(li)
}

func (cc *CtlCli) Init(addr, taskAddr, name, key string) error {
	//
	cc.ctx, cc.cancel = context.WithCancel(context.TODO())
	//
	cc.wg.Add(1)
	go func() {
		cc.init4Task(taskAddr)
		cc.wg.Done()
	}()
	//
	ctx, cancel := context.WithTimeout(cc.ctx, time.Second*5)
	cn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		cancel()
		return fmt.Errorf("DialContext err:%s", err.Error())
	}
	cl := msg.NewToolsManagerClient(cn)
	cc.info = msg.ClientInfo{
		Name: name,
		Key:  key,
	}
	cc.cli = cl
	cc.addr = addr
	return nil
}

func (cc *CtlCli) Register() error {
	replay, err := cc.cli.Register(cc.ctx, &msg.ReqRegister{
		Cli:      &cc.info,
		TaskAddr: cc.taskSrv.taskAddr,
	})
	if err != nil {
		LogInfo("regist net err:%s", err.Error())
		return err
	}
	if replay.Rc.Rc == msg.RespCode_rc_OK {
		LogInfo("Register ok")
		return nil
	}
	LogErr("Register err:%s", replay.Rc.RespMsg)
	return nil
}

var (
	gc = &CtlCli{}
	//
	saddr = flag.String("s", ":9876", "address of server")
	key   = flag.String("k", "123456", "key of this running")

	taskAddr = flag.String("t", ":9899", "listener for wait task")
)

func RunClient() {
	flag.Parse()
	err := gc.Init(*saddr, *taskAddr, "dumpling-1", "123456")
	if err != nil {
		LogFatal("Init err:%s", err.Error())
	}
	err = gc.Register()
	if err != nil {
		LogFatal("Regist err:%s", err.Error())
	}

	gc.wg.Wait()
}

func main() {
	RunClient()
}
