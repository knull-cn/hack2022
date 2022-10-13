package srv

import (
	"context"
	"flag"
	"fmt"
	"github.com/knullhhf/hack22/cli"
	. "github.com/knullhhf/hack22/logger"
	mnet "github.com/knullhhf/hack22/net"
	"github.com/knullhhf/hack22/net/msg"
	"github.com/knullhhf/hack22/net/storage"
	"github.com/knullhhf/hack22/task"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	mydump2 "github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/restore"
	"github.com/pingcap/tidb/util/filter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

type Server struct {
	msg.UnimplementedToolsManagerServer
	//
	addr     string
	taskAddr string
	//
	cliMtx        sync.Mutex
	writerClients map[string]*cli.WriterClient
	//
	taskMtx        sync.Mutex
	reportInterval int // second
	//
	ctx    context.Context
	cancel context.CancelFunc
	//
	wg sync.WaitGroup
}

func (cs *Server) exit() {
	cs.cancel()
}

func (cs *Server) waitTaskCli(lis net.Listener) {
	LogInfo("waitTaskCli running")
	defer cs.wg.Done()
	for {
		con, err := lis.Accept()
		if err != nil {
			LogErr("accept err:%s", err.Error())
			cs.exit()
		}
		// auth;
		var buf [1024]byte
		n, err := io.LimitReader(con, 1024).Read(buf[:])
		if err != nil {
			LogErr("accept err:%s", err.Error())
			return
		}

		key := string(buf[:n])
		cs.taskMtx.Lock()
		if !strings.HasPrefix(key, "task") {
			LogErr("parse packet err:%s", err.Error())
			return
		}

		metas := strings.Split(key, ":")
		ct, ok := cs.writerClients[metas[1]]
		if !ok {
			cs.taskMtx.Unlock()
			LogErr("not found key(%s) in pending", key)
			return
		}
		cs.taskMtx.Unlock()
		// change state to task;
		ct.RunningTasks[metas[2]] = ct.PendingTasks[metas[2]]
		ct.RunningTasks[metas[2]].DumpState = msg.TaskState_ts_Create
		//ct.RunningTasks[metas[2]].LightState = msg.TaskState_ts_Create
		delete(ct.PendingTasks, metas[2])

		ct.RunningTasks[metas[2]].WriterSource.Reader.Connection = con
		// done;
		cs.wg.Add(1)
		go cs.task4lightning(ct.RunningTasks[metas[2]])
	}
}

func (cs *Server) task4lightning(task *task.MigrateTask) {
	defer cs.wg.Done()
	dbMetas := []*mydump2.MDDatabaseMeta{}
	table := task.Table
	//generate db metadata
	dbMetas = append(dbMetas, &mydump2.MDDatabaseMeta{
		Name: table.Database,
		SchemaFile: mydump2.FileInfo{
			TableName: filter.Table{
				Name: table.Database,
			},
		},
		Tables: []*mydump2.MDTableMeta{
			{
				DB:   table.Database,
				Name: table.Name,
				DataFiles: []mydump2.FileInfo{{
					TableName: filter.Table{
						Schema: table.Database,
						Name:   table.Name,
					},
					FileMeta: mydump2.SourceFileMeta{
						Path:        "middleware_pass.mcloud_middleware_env.000000000.csv",
						Type:        mydump2.SourceTypeCSV,
						Compression: mydump2.CompressionNone,
						SortKey:     "000000000",
						FileSize:    1129,
					},
				}},
			},
		},
		Views: nil,
	})
	ctx := context.TODO()
	db, err := restore.DBFromConfig(ctx, task.Config.TiDB)
	if err != nil {
		log.L().Error("DB ERROR")
		return
	}
	g := glue.NewExternalTiDBGlue(db, task.Config.TiDB.SQLMode)

	param := &restore.ControllerParam{
		DBMetas:           dbMetas,
		Status:            &restore.LightningStatus{},
		DumpFileStorage:   task.WriterSource,
		OwnExtStorage:     false,
		Glue:              g,
		CheckpointStorage: nil,
		CheckpointName:    "",
	}
	procedure, err := restore.NewRestoreController(ctx, task.Config, param)
	if err != nil {
		log.L().Error("restore failed", log.ShortError(err))
		return
	}
	defer procedure.Close()
	err = procedure.Run(ctx)
	if err != nil {
		LogErr("migrate task error:%v", err)
	}
}

func (cs *Server) reportLoop() {
	tick := time.Tick(time.Second * time.Duration(cs.reportInterval))
	for {
		select {
		case <-tick:
			cs.ReportState()
		case <-cs.ctx.Done():
			break
		}
	}
	cs.wg.Done()
}

func (cs *Server) addTask(task *task.MigrateTask) error {
	cli, ok := cs.writerClients[task.ClientName]
	if !ok {
		//
		return fmt.Errorf("%s not found", task.ClientName)
	}
	_, ok = cli.PendingTasks[task.Key]
	if ok {
		LogInfo("have added the task(%s) before", task.Name)
		return nil
	}

	_, ok = cli.RunningTasks[task.Key]
	if ok {
		LogInfo("the task(%s) is running", task.Name)
		return nil
	}

	_, ok = cli.FinishedTasks[task.Key]
	if ok {
		LogInfo("the task(%s) is finished", task.Name)
		return nil
	}
	cli.PendingTasks[task.Name] = task
	task.WriterSource = &storage.SocketStorage{Reader: &storage.SocketStorageReader{TaskManagerClient: cli.TaskManagerClient, TaskInfo: &msg.ReqNewTask{
		Cli: cli.Client,
		Task: &msg.TaskInfo{
			Name: task.Name,
			Key:  task.Key,
			Db:   task.Table.Database,
			Tbl:  task.Table.Name,
		},
	}}}
	return nil
}

func (cs *Server) handle() {
	for {
		for _, writer := range cs.writerClients {
			for _, task := range writer.PendingTasks {
				if task.LightState != msg.TaskState_ts_Create {
					task.LightState = msg.TaskState_ts_Create
					cs.newTask(writer.Client, &msg.TaskInfo{
						Name: task.Name,
						Key:  task.Key,
						Db:   task.Table.Database,
						Tbl:  task.Table.Name,
					})
				}
			}
		}
	}
}
func (cs *Server) findCli(name, key string) (*cli.WriterClient, error) {
	cs.cliMtx.Lock()
	lcli, ok := cs.writerClients[name]
	cs.cliMtx.Unlock()
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return lcli, nil
}

func (cs *Server) Register(ctx context.Context, in *msg.ReqRegister) (*msg.ReplyRegister, error) {
	client := in.Cli
	//
	LogTraceJson("Register %s", in)
	// init taskManager;
	gc, err := grpc.DialContext(cs.ctx, in.Cli.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		err := fmt.Errorf("dial context task addr(%s) err:%w", in.Cli.GetAddress(), err)
		LogErr(err.Error())
		return nil, err
	}
	//
	cs.cliMtx.Lock()
	cs.writerClients[in.Cli.Name] = &cli.WriterClient{
		Client:            client,
		TaskManagerClient: msg.NewTaskManagerClient(gc),
		PendingTasks:      map[string]*task.MigrateTask{},
		FinishedTasks:     map[string]*task.MigrateTask{},
		RunningTasks:      map[string]*task.MigrateTask{},
	}
	cs.cliMtx.Unlock()
	LogInfo("new tool(%s) registered", client.GetName())
	return &msg.ReplyRegister{Rc: mnet.DefaultOkReplay()}, nil
}

func (cs *Server) newTask(cli *msg.ClientInfo, t *msg.TaskInfo) {
	obj, err := cs.findCli(cli.GetName(), cli.GetKey())
	if err != nil {
		err := fmt.Errorf("findCli(%s) err:%w", cli.GetName(), err)
		LogErr(err.Error())
		return
	}
	r := msg.ReqNewTask{
		Cli:  cli,
		Task: t,
		Server: &msg.ServerInfo{
			TaskAddress: cs.taskAddr,
		},
	}
	reply, err := obj.TaskManagerClient.NewTask(cs.ctx, &r)
	if err != nil {
		err := fmt.Errorf("NewTask(%s) by cli(%s) err:%w", t.Name, cli.GetName(), err)
		LogErr(err.Error())
		return
	}
	_ = reply
	LogInfo("newTask (%s-%s) to init finish.", cli.GetName(), t.GetName())
}

func (cs *Server) startTask(cli *msg.ClientInfo, t *msg.TaskInfo) {
	obj, err := cs.findCli(cli.GetName(), cli.GetKey())
	if err != nil {
		err := fmt.Errorf("findCli(%s) err:%w", cli.GetName(), err)
		LogErr(err.Error())
		return
	}
	r := msg.ReqNewTask{
		Cli:  cli,
		Task: t,
	}
	reply, err := obj.TaskManagerClient.NewTask(cs.ctx, &r)
	if err != nil {
		err := fmt.Errorf("NewTask(%s) by cli(%s) err:%w", t.Name, cli.GetName(), err)
		LogErr(err.Error())
		return
	}
	_ = reply
	LogInfo("newTask (%s-%s) to init finish.", cli.GetName(), t.GetName())
}

func (cs *Server) ReportState() {
	for _, cc := range cs.writerClients {
		cs.wg.Add(1)
		go func(c *cli.WriterClient) {
			cs.reportCliTasks(c, c.RunningTasks)
			cs.wg.Done()
		}(cc)
	}
}

func (cs *Server) reportCliTasks(c *cli.WriterClient, tasks map[string]*task.MigrateTask) {
	for _, t := range tasks {
		reply, err := c.TaskManagerClient.ReportState(cs.ctx, &msg.ReqReport{
			Cli: c.Client,
			Task: &msg.TaskInfo{
				Name: t.Name,
				Key:  t.Key,
			},
		})
		if err != nil {
			LogErr("ReportState(%s-%s) err:%s", c.Client.Name, t.Name, err.Error())
		} else {
			LogInfo("ReportState(%s-%s)  state(%v):%s", c.Client.Name, t.Name, reply.GetState(), reply.GetProgress())
		}
	}
}

func (cs *Server) Init(taskAddress string, sec int) {

	taskListener, err := net.Listen("tcp", taskAddress)
	if err != nil {
		LogFatal("listen on '%s' failed", taskAddress)
	}
	cs.ctx, cs.cancel = context.WithCancel(context.TODO())
	cs.reportInterval = sec
	cs.taskAddr = taskAddress
	cs.writerClients = map[string]*cli.WriterClient{}

	cs.wg.Add(1)
	go cs.waitTaskCli(taskListener)
	go cs.handle()
	cs.wg.Add(1)
	go cs.reportLoop()
}

var (
	LightningServer = &Server{}
	//
	address     = flag.String("l", ":9876", "address of listener")
	taskAddress = flag.String("t", ":9877", "listener of task ")
	report      = flag.Int("r", 1, "report of interval second")
)

func RunServer() {
	flag.Parse()
	LightningServer.Init(*taskAddress, 1)
	runGrpcServer(*address)
}

func runGrpcServer(addr string) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		LogFatal("listen err:%s", err.Error())
	}
	server := grpc.NewServer()
	msg.RegisterToolsManagerServer(server, LightningServer)
	if err := server.Serve(listener); err != nil {
		LogFatal("failed to serve: %v", err)
	}
}
