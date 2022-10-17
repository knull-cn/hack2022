package client

import (
	"context"
	"fmt"
	"github.com/knullhhf/hack22/logger"
	net2 "github.com/knullhhf/hack22/pkg/net"
	msg2 "github.com/knullhhf/hack22/pkg/net/msg"
	storage2 "github.com/knullhhf/hack22/pkg/net/storage"
	"github.com/pingcap/tidb/dumpling/export"
	"go.uber.org/zap"
	"net"
	"os"
	"sync"
)

type TaskServiceInterface interface {
	NewTask(ctx context.Context, in *msg2.ReqNewTask) (*msg2.ReplyNewTask, error)
	StartTask(ctx context.Context, in *msg2.ReqNewTask) (*msg2.ReplyNewTask, error)
	ReportState(ctx context.Context, in *msg2.ReqReport) (*msg2.ReplyReport, error)
}

type TaskService struct {
	msg2.UnimplementedTaskManagerServer
	cli          *msg2.ClientInfo
	tasks        map[string]*cliTask
	writeSignals map[string]*sync.WaitGroup
	wg           *sync.WaitGroup
	ctx          context.Context
}

func (ts *TaskService) NewTask(ctx context.Context, in *msg2.ReqNewTask) (*msg2.ReplyNewTask, error) {
	logger.LogTraceJsonf("NewTask %s", in)

	tc, err := net.Dial("tcp", in.GetServer().GetTaskAddress())
	if err != nil {
		return nil, fmt.Errorf("dial '%s' err:%s", in.GetCli().GetAddress(), err.Error())
	}

	k := net2.SocketKey(in.Cli.Name, in.Task.Key)
	_, err = tc.Write([]byte(k))
	if err != nil {
		return nil, fmt.Errorf("write err:%w", err)
	}

	logger.LogTracef("write key '%s'", k)
	ct := cliTask{
		name:  in.Task.Name,
		con:   tc,
		info:  *in.Task,
		state: msg2.TaskState_ts_Create,
	}
	ts.tasks[ct.name] = &ct
	//
	ts.writeSignals[in.Task.Name] = &sync.WaitGroup{}
	ts.writeSignals[in.Task.Name].Add(1)
	go ts.DumpTableData(&ct)
	return &msg2.ReplyNewTask{Rc: net2.DefaultOkReplay()}, nil
}

// StartTask start write data
func (ts *TaskService) StartTask(ctx context.Context, in *msg2.ReqNewTask) (*msg2.ReplyNewTask, error) {
	logger.LogTraceJsonf("StartTask %s", in)
	ts.writeSignals[in.Task.Name].Done()
	return &msg2.ReplyNewTask{Rc: net2.DefaultOkReplay()}, nil
}

func (ts *TaskService) ReportState(ctx context.Context, in *msg2.ReqReport) (*msg2.ReplyReport, error) {
	t := ts.tasks[in.GetTask().GetName()]

	rr := msg2.ReplyReport{
		Rc:       net2.DefaultOkReplay(),
		State:    t.state,
		Progress: t.progress,
	}
	return &rr, nil
}

func (cc *TaskService) DumpTableData(task *cliTask) {
	logger.LogInfof("DumpData(%s) waiting write signals....", task.name)
	cc.writeSignals[task.name].Wait()
	conf := export.DefaultConfig()
	logger.LogInfof("DumpData(%s) start write ....", task.name)
	extStorage := &storage2.SocketStorage{
		Writer: &storage2.SocketStorageWriter{
			Connection: task.con,
		},
	}
	conf.User = task.info.Source.Username
	conf.Password = task.info.Source.Password
	conf.Port = int(task.info.Source.Port)
	conf.Host = task.info.Source.Host
	conf.SQL = fmt.Sprintf("select * from `%s`.`%s`", task.info.Source.Db, task.info.Source.Tbl)
	conf.FileType = "csv"
	conf.ExtStorage = extStorage
	conf.CsvSeparator = ","
	conf.CsvDelimiter = "\""
	conf.StatementSize = 2000000
	conf.FileSize = 1024 * 1024 * 1024 //need to justify
	ctx := context.TODO()
	dumper, err := export.NewDumper(ctx, conf)
	if err != nil {
		fmt.Printf("\ncreate dumper failed: %s\n", err.Error())
		os.Exit(1)
	}
	err = dumper.Dump()
	//time.Sleep(60 * time.Second)
	task.con.Close()
	if err != nil {
		dumper.L().Error("dump failed error stack info", zap.Error(err))
		fmt.Printf("\ndump failed: %s\n", err.Error())
		os.Exit(1)
	}

	logger.LogInfo("close connection success")

}
