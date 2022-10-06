package main

import (
	"context"
	"fmt"
	"gitee.com/knullhhf/hack22/logger"
	mnet "gitee.com/knullhhf/hack22/net"
	"gitee.com/knullhhf/hack22/net/msg"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type taskService struct {
	msg.UnimplementedTaskManagerServer
	cli      *msg.ClientInfo
	taskAddr string
	tasks    map[string]*cliTask
	wg       *sync.WaitGroup
	ctx      context.Context
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

	k := mnet.SocketKey(in.Cli.Name, in.Task.TaskName, in.Task.TaskKey)
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
	ts.wg.Add(1)
	go ts.DumpData(&ct)
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
	defer cc.wg.Done()
	// select from source data;
	// dump data to

	// TODO : set state here. just for test this;
	task.state = msg.TaskState_ts_Dumpling
	var idx int64
	for {
		time.Sleep(time.Second)
		task.progress = fmt.Sprintf("%d", atomic.AddInt64(&idx, 1))
		task.con.Write([]byte(fmt.Sprintf("run - %d.\n", idx)))
	}
}
