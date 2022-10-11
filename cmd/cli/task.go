package main

import (
	"context"
	"fmt"
	"github.com/knullhhf/hack22/logger"
	mnet "github.com/knullhhf/hack22/net"
	"github.com/knullhhf/hack22/net/msg"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
)

type taskService struct {
	msg.UnimplementedTaskManagerServer
	cli          *msg.ClientInfo
	taskAddr     string
	tasks        map[string]*cliTask
	writeSignals map[string]*sync.WaitGroup
	wg           *sync.WaitGroup
	ctx          context.Context
}

// func (ts *taskService) mustEmbedUnimplementedTaskManagerServer() {
// 	// TODO implement me
// 	panic("implement me")
// }

func (ts *taskService) NewTask(ctx context.Context, in *msg.ReqNewTask) (*msg.ReplyNewTask, error) {
	logger.LogTraceJson("NewTask %s", in)
	// TODO : cli check;

	// task check.
	tc, err := net.Dial("tcp", in.GetTaskAddr())
	if err != nil {
		return nil, fmt.Errorf("dial '%s' err:%s", in.GetTaskAddr(), err.Error())
	}

	k := mnet.SocketKey(in.Cli.Name, in.Task.TaskKey)
	_, err = tc.Write([]byte(k))
	if err != nil {
		return nil, fmt.Errorf("write err:%w", err)
	}

	logger.LogTrace("write key '%s'", k)
	ct := cliTask{
		name:  in.Task.TaskName,
		con:   tc,
		info:  *in.Task,
		state: msg.TaskState_ts_Create,
	}
	ts.tasks[ct.name] = &ct
	//
	ts.writeSignals[in.Task.TaskName] = &sync.WaitGroup{}
	ts.writeSignals[in.Task.TaskName].Add(1)
	go ts.DumpData(&ct)
	return &msg.ReplyNewTask{Rc: mnet.DefaultOkReplay()}, nil
}

// StartTask start write data
func (ts *taskService) StartTask(ctx context.Context, in *msg.ReqNewTask) (*msg.ReplyNewTask, error) {
	logger.LogTraceJson("NewTask %s", in)
	ts.writeSignals[in.Task.TaskName].Done()
	return &msg.ReplyNewTask{Rc: mnet.DefaultOkReplay()}, nil
}

func (ts *taskService) ReportState(ctx context.Context, in *msg.ReqReport) (*msg.ReplyReport, error) {
	t := ts.tasks[in.GetTask().GetTaskName()]

	rr := msg.ReplyReport{
		Rc:       mnet.DefaultOkReplay(),
		State:    t.state,
		Progress: t.progress,
	}
	return &rr, nil
}

func (cc *taskService) DumpData(task *cliTask) {
	logger.LogInfo("DumpData(%s) running....", task.name)
	defer cc.writeSignals[task.name].Wait()

	task.state = msg.TaskState_ts_Dumpling
	var idx int64

	task.progress = fmt.Sprintf("%d", atomic.AddInt64(&idx, 1))
	file, err := os.Open("/Users/mikechengwei/Downloads/env_table/middleware_pass.mcloud_middleware_env.000000000.csv")
	if err != nil {
		fmt.Println(err)
		return
	}
	sendBuffer := make([]byte, 1024)
	for {
		n, err := file.Read(sendBuffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.LogErr("dump data error:%v", err)
			break
		}
		if n == 0 {
			break
		}
		task.con.Write(sendBuffer[:n])
	}
	task.con.Close()
	logger.LogInfo("write success")

}
