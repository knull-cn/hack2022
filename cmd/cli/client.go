package main

import (
	"context"
	"flag"
	"fmt"
	. "github.com/knullhhf/hack22/logger"
	"github.com/knullhhf/hack22/net/msg"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"sync"
	"time"
)

type Client struct {
	serverAddress  string
	info           msg.ClientInfo
	serverGrpcTool msg.ToolsManagerClient
	taskService    TaskService

	ctx    context.Context
	cancel context.CancelFunc
}

type cliTask struct {
	name     string
	con      net.Conn
	info     msg.TaskInfo
	state    msg.TaskState
	progress string
}

func (cc *Client) startTaskListen() {
	cc.taskService.ctx = cc.ctx
	cc.taskService.tasks = map[string]*cliTask{}
	cc.taskService.writeSignals = map[string]*sync.WaitGroup{}
	clientGrpcServer := grpc.NewServer()
	msg.RegisterTaskManagerServer(clientGrpcServer, &cc.taskService)
	controlListener, err := net.Listen("tcp", cc.info.GetAddress())
	if err != nil {
		LogFatal("listen taskAddr(%s) err:%s", cc.info.GetAddress(), err.Error())
	}
	err = clientGrpcServer.Serve(controlListener)
	if err != nil {
		LogErr("start client task listen err:%v", err)
	}
}

func (cc *Client) Init(serverAddress, clientAddress, name, key string) error {
	cc.ctx, cc.cancel = context.WithCancel(context.TODO())
	ctx, cancel := context.WithTimeout(cc.ctx, time.Second*5)
	cn, err := grpc.DialContext(ctx, serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		cancel()
		return fmt.Errorf("DialContext err:%s", err.Error())
	}
	serverGrpcTool := msg.NewToolsManagerClient(cn)
	cc.info = msg.ClientInfo{
		Name:    name,
		Key:     key,
		Address: clientAddress,
	}
	cc.serverGrpcTool = serverGrpcTool
	cc.serverAddress = serverAddress
	return nil
}

func (cc *Client) Register() error {
	replay, err := cc.serverGrpcTool.Register(cc.ctx, &msg.ReqRegister{
		Cli: &cc.info,
	})
	if err != nil {
		LogInfo("register net err:%s", err.Error())
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
	client        = &Client{}
	serverAddress = flag.String("s", ":9876", "address of server")
	key           = flag.String("k", "123456", "key of this running")

	clientAddress = flag.String("t", ":9899", "listener for wait task")
)

func RunClient() {
	flag.Parse()
	err := client.Init(*serverAddress, *clientAddress, "dumpling-1", "123456")
	if err != nil {
		LogFatal("Init err:%s", err.Error())
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		client.startTaskListen()
		wg.Done()
	}()
	err = client.Register()
	if err != nil {
		LogFatal("Regist err:%s", err.Error())
	}

	wg.Add(1)
	wg.Wait()

}

func main() {
	RunClient()
}
