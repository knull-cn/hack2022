package main

import (
	"context"
	"fmt"
	"github.com/knullhhf/hack22/logger"
	mnet "github.com/knullhhf/hack22/net"
	"github.com/knullhhf/hack22/net/msg"
	"github.com/knullhhf/hack22/net/storage"
	"github.com/pingcap/tidb/dumpling/export"
	"go.uber.org/zap"
	"net"
	"os"
	"sync"
)

type TaskServiceInterface interface {
	NewTask(ctx context.Context, in *msg.ReqNewTask) (*msg.ReplyNewTask, error)
	StartTask(ctx context.Context, in *msg.ReqNewTask) (*msg.ReplyNewTask, error)
	ReportState(ctx context.Context, in *msg.ReqReport) (*msg.ReplyReport, error)
}

type TaskService struct {
	msg.UnimplementedTaskManagerServer
	cli          *msg.ClientInfo
	tasks        map[string]*cliTask
	writeSignals map[string]*sync.WaitGroup
	wg           *sync.WaitGroup
	ctx          context.Context
}

func (ts *TaskService) NewTask(ctx context.Context, in *msg.ReqNewTask) (*msg.ReplyNewTask, error) {
	logger.LogTraceJson("NewTask %s", in)

	tc, err := net.Dial("tcp", in.GetServer().GetTaskAddress())
	if err != nil {
		return nil, fmt.Errorf("dial '%s' err:%s", in.GetCli().GetAddress(), err.Error())
	}

	k := mnet.SocketKey(in.Cli.Name, in.Task.Key)
	_, err = tc.Write([]byte(k))
	if err != nil {
		return nil, fmt.Errorf("write err:%w", err)
	}

	logger.LogTrace("write key '%s'", k)
	ct := cliTask{
		name:  in.Task.Name,
		con:   tc,
		info:  *in.Task,
		state: msg.TaskState_ts_Create,
	}
	ts.tasks[ct.name] = &ct
	//
	ts.writeSignals[in.Task.Name] = &sync.WaitGroup{}
	ts.writeSignals[in.Task.Name].Add(1)
	go ts.DumpTableData(&ct)
	return &msg.ReplyNewTask{Rc: mnet.DefaultOkReplay()}, nil
}

// StartTask start write data
func (ts *TaskService) StartTask(ctx context.Context, in *msg.ReqNewTask) (*msg.ReplyNewTask, error) {
	logger.LogTraceJson("StartTask %s", in)
	ts.writeSignals[in.Task.Name].Done()
	return &msg.ReplyNewTask{Rc: mnet.DefaultOkReplay()}, nil
}

func (ts *TaskService) ReportState(ctx context.Context, in *msg.ReqReport) (*msg.ReplyReport, error) {
	t := ts.tasks[in.GetTask().GetName()]

	rr := msg.ReplyReport{
		Rc:       mnet.DefaultOkReplay(),
		State:    t.state,
		Progress: t.progress,
	}
	return &rr, nil
}

func (cc *TaskService) DumpTableData(task *cliTask) {
	logger.LogInfo("DumpData(%s) waiting write signals....", task.name)
	cc.writeSignals[task.name].Wait()
	conf := export.DefaultConfig()

	extStorage := &storage.SocketStorage{
		Writer: &storage.SocketStorageWriter{
			Connection: task.con,
		},
	}
	conf.ExtStorage = extStorage
	ctx := context.TODO()
	dumper, err := export.NewDumper(ctx, nil)
	if err != nil {
		fmt.Printf("\ncreate dumper failed: %s\n", err.Error())
		os.Exit(1)
	}
	err = dumper.Dump()
	_ = dumper.Close()
	if err != nil {
		dumper.L().Error("dump failed error stack info", zap.Error(err))
		fmt.Printf("\ndump failed: %s\n", err.Error())
		os.Exit(1)
	}

	//task.state = msg.TaskState_ts_Dumpling
	//var idx int64
	//
	//task.progress = fmt.Sprintf("%d", atomic.AddInt64(&idx, 1))
	//file, err := os.Open("/Users/mikechengwei/Downloads/env_table/middleware_pass.mcloud_middleware_env.000000000.csv")
	//if err != nil {
	//	fmt.Println(err)
	//	return
	//}
	//sendBuffer := make([]byte, 1024)
	//for {
	//	n, err := file.Read(sendBuffer)
	//	if err == io.EOF {
	//		break
	//	}
	//	if err != nil {
	//		logger.LogErr("dump data error:%v", err)
	//		break
	//	}
	//	if n == 0 {
	//		break
	//	}
	//	task.con.Write(sendBuffer[:n])
	//}
	//err = task.con.Close()
	//if err != nil {
	//	logger.LogErr("close connection error:%v", err)
	//}
	logger.LogInfo("close connection success")

}
