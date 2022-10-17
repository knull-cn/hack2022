package client

import (
	"context"
	"flag"
	"fmt"
	. "github.com/knullhhf/hack22/logger"
	msg2 "github.com/knullhhf/hack22/pkg/net/msg"
	"github.com/knullhhf/hack22/pkg/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"sync"
	"time"
)

type Client struct {
	serverAddress  string
	info           msg2.ClientInfo
	serverGrpcTool msg2.ToolsManagerClient
	taskService    TaskService

	ctx    context.Context
	cancel context.CancelFunc
}

type cliTask struct {
	name     string
	con      net.Conn
	info     msg2.TaskInfo
	state    msg2.TaskState
	progress string
}

func (cc *Client) startTaskListen(wg *sync.WaitGroup) {
	cc.taskService.ctx = cc.ctx
	cc.taskService.tasks = map[string]*cliTask{}
	cc.taskService.writeSignals = map[string]*sync.WaitGroup{}
	clientGrpcServer := grpc.NewServer()
	msg2.RegisterTaskManagerServer(clientGrpcServer, &cc.taskService)
	controlListener, err := net.Listen("tcp", cc.info.GetAddress())
	if err != nil {
		LogFatalf("listen taskAddr(%s) err:%s", cc.info.GetAddress(), err.Error())
	}
	wg.Done()
	err = clientGrpcServer.Serve(controlListener)
	if err != nil {
		LogErrf("start client task listen err:%v", err)
	}
}

func (cc *Client) Init(serverAddress, clientAddress, name string) error {
	cc.ctx, cc.cancel = context.WithCancel(context.TODO())
	ctx, cancel := context.WithTimeout(cc.ctx, time.Second*5)
	defer cancel()
	cn, err := grpc.DialContext(ctx, serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		cancel()
		return fmt.Errorf("subscribe server grpc err:%v", err)
	}
	serverGrpcTool := msg2.NewToolsManagerClient(cn)
	key := utils.GenerateClientUUid()
	cc.info = msg2.ClientInfo{
		Name:    name,
		Key:     key,
		Address: clientAddress,
	}
	cc.serverGrpcTool = serverGrpcTool
	cc.serverAddress = serverAddress
	return nil
}

func (cc *Client) Register() error {
	replay, err := cc.serverGrpcTool.Register(cc.ctx, &msg2.ReqRegister{
		Cli: &cc.info,
	})
	if err != nil {
		LogInfof("register client[%s] err:%s", cc.info.Name, err.Error())
		return err
	}
	if replay.Rc.Rc == msg2.RespCode_rc_OK {
		LogInfof("Register success")
		return nil
	}
	return nil
}

var (
	client        = &Client{}
	serverAddress = flag.String("s", ":9876", "address of server")
	key           = flag.String("k", "123456", "key of this running")

	clientAddress = flag.String("t", ":9899", "listener for wait task")
)

func RunClient() {
	InitLog()
	flag.Parse()
	err := client.Init(*serverAddress, *clientAddress, "dumpling-1")
	if err != nil {
		LogFatalf("Init err:%s", err.Error())
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		client.startTaskListen(&wg)
	}()
	wg.Wait()
	err = client.Register()
	if err != nil {
		LogFatalf("Regist err:%s", err.Error())
	}

	wg.Add(1)
	wg.Wait()

}
