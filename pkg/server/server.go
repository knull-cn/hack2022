package server

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	. "github.com/knullhhf/hack22/logger"
	task2 "github.com/knullhhf/hack22/pkg/models/task"
	net2 "github.com/knullhhf/hack22/pkg/net"
	msg2 "github.com/knullhhf/hack22/pkg/net/msg"
	storage2 "github.com/knullhhf/hack22/pkg/net/storage"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	mydump2 "github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/restore"
	"github.com/pingcap/tidb/util/filter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Server struct {
	msg2.UnimplementedToolsManagerServer
	//
	addr     string
	taskAddr string
	//
	cliMtx        sync.Mutex
	writerClients map[string]*WriterClient
	//
	taskMtx        sync.Mutex
	reportInterval int // second
	//
	ctx    context.Context
	cancel context.CancelFunc
	//
	wg sync.WaitGroup
}

func (server *Server) exit() {
	server.cancel()
}

func (server *Server) waitTaskCli(lis net.Listener) {
	LogInfo("waitTaskCli running")
	defer server.wg.Done()
	for {
		con, err := lis.Accept()
		if err != nil {
			LogErrf("accept err:%s", err.Error())
			server.exit()
		}
		// auth;
		var buf [1024]byte
		n, err := io.LimitReader(con, 1024).Read(buf[:])
		if err != nil {
			LogErrf("accept err:%s", err.Error())
			return
		}

		key := string(buf[:n])
		server.taskMtx.Lock()
		if !strings.HasPrefix(key, "task") {
			LogErrf("parse packet err:%s", err.Error())
			return
		}

		metas := strings.Split(key, ":")
		ct, ok := server.writerClients[metas[1]]
		if !ok {
			server.taskMtx.Unlock()
			LogErrf("not found key(%s) in pending", key)
			return
		}
		server.taskMtx.Unlock()
		// change state to task;
		ct.RunningTasks[metas[2]] = ct.PendingTasks[metas[2]]
		ct.RunningTasks[metas[2]].DumpState = msg2.TaskState_ts_Create
		//ct.RunningTasks[metas[2]].LightState = msg.TaskState_ts_Create
		delete(ct.PendingTasks, metas[2])

		ct.RunningTasks[metas[2]].WriterSource.Reader.Connection = con
		// done;
		server.wg.Add(1)
		go server.importData(ct.RunningTasks[metas[2]])
	}
}

func (server *Server) ListWriteClient() []*msg2.ClientInfo {
	var v []*msg2.ClientInfo
	for _, value := range server.writerClients {
		v = append(v, value.Client)
	}
	return v
}

func (server *Server) importData(task *task2.MigrateTask) {
	defer server.wg.Done()
	dbMetas := []*mydump2.MDDatabaseMeta{}
	table := task.Target
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
						Path:        "middleware_pass.mcloud_middleware_env.000000000.serverv",
						Type:        mydump2.SourceTypeCSV,
						Compression: mydump2.CompressionNone,
						SortKey:     "000000000",
						FileSize:    1024 * 1024 * 1024, //need to justify
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
		LogErrf("migrate task error:%v", err)
	}
}

func (server *Server) reportLoop() {
	tick := time.Tick(time.Second * time.Duration(server.reportInterval))
	for {
		select {
		case <-tick:
			server.ReportState()
		case <-server.ctx.Done():
			break
		}
	}
	server.wg.Done()
}

func (server *Server) addTask(task *task2.MigrateTask) error {
	cli, ok := server.writerClients[task.ClientName]
	if !ok {
		//
		return fmt.Errorf("%s not found", task.ClientName)
	}
	_, ok = cli.PendingTasks[task.Key]
	if ok {
		LogInfof("have added the task(%s) before", task.Name)
		return nil
	}

	_, ok = cli.RunningTasks[task.Key]
	if ok {
		LogInfof("the task(%s) is running", task.Name)
		return nil
	}

	_, ok = cli.FinishedTasks[task.Key]
	if ok {
		LogInfof("the task(%s) is finished", task.Name)
		return nil
	}
	cli.PendingTasks[task.Name] = task
	task.WriterSource = &storage2.SocketStorage{Reader: &storage2.SocketStorageReader{TaskManagerClient: cli.TaskManagerClient, TaskInfo: &msg2.ReqNewTask{
		Cli: cli.Client,
		Task: &msg2.TaskInfo{
			Name: task.Name,
			Key:  task.Key,
			Source: &msg2.TableInfo{
				Host:     task.Source.Host,
				Port:     task.Source.Port,
				Username: task.Source.Username,
				Password: task.Source.Password,
				Db:       task.Source.Database,
				Tbl:      task.Source.Name,
			},
			Target: &msg2.TableInfo{
				Db:  task.Target.Database,
				Tbl: task.Target.Name,
			},
		},
	}}}
	return nil
}

func (server *Server) handle() {
	for {
		for _, writer := range server.writerClients {
			for _, task := range writer.PendingTasks {
				if task.LightState != msg2.TaskState_ts_Create {
					task.LightState = msg2.TaskState_ts_Create
					server.newTask(writer.Client, &msg2.TaskInfo{
						Name: task.Name,
						Key:  task.Key,
						Source: &msg2.TableInfo{
							Host:     task.Source.Host,
							Port:     task.Source.Port,
							Username: task.Source.Username,
							Password: task.Source.Password,
							Db:       task.Source.Database,
							Tbl:      task.Source.Name,
						},
						Target: &msg2.TableInfo{
							Db:  task.Target.Database,
							Tbl: task.Target.Name,
						},
					})
				}
			}
		}
	}
}
func (server *Server) findCli(name, key string) (*WriterClient, error) {
	server.cliMtx.Lock()
	lcli, ok := server.writerClients[name]
	server.cliMtx.Unlock()
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return lcli, nil
}

func (server *Server) Register(ctx context.Context, in *msg2.ReqRegister) (*msg2.ReplyRegister, error) {
	client := in.Cli
	//
	LogTraceJsonf("Register %v", in)
	// init client grpc manager
	gc, err := grpc.DialContext(server.ctx, in.Cli.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		err := fmt.Errorf("init client[addr:%s] grpc manaager,err:%w", in.Cli.GetAddress(), err)
		LogErrf(err.Error())
		return nil, err
	}

	server.cliMtx.Lock()
	server.writerClients[in.Cli.Key] = &WriterClient{
		Client:            client,
		TaskManagerClient: msg2.NewTaskManagerClient(gc),
		PendingTasks:      map[string]*task2.MigrateTask{},
		FinishedTasks:     map[string]*task2.MigrateTask{},
		RunningTasks:      map[string]*task2.MigrateTask{},
	}
	server.cliMtx.Unlock()
	LogInfof("new client(%s) registered", client.GetName())
	return &msg2.ReplyRegister{Rc: net2.DefaultOkReplay()}, nil
}

func (server *Server) newTask(cli *msg2.ClientInfo, t *msg2.TaskInfo) {
	obj, err := server.findCli(cli.GetName(), cli.GetKey())
	if err != nil {
		err := fmt.Errorf("findCli(%s) err:%w", cli.GetName(), err)
		LogErrf(err.Error())
		return
	}
	r := msg2.ReqNewTask{
		Cli:  cli,
		Task: t,
		Server: &msg2.ServerInfo{
			TaskAddress: server.taskAddr,
		},
	}
	reply, err := obj.TaskManagerClient.NewTask(server.ctx, &r)
	if err != nil {
		err := fmt.Errorf("NewTask(%s) by cli(%s) err:%w", t.Name, cli.GetName(), err)
		LogErrf(err.Error())
		return
	}
	_ = reply
	LogInfof("newTask (%s-%s) to init finish.", cli.GetName(), t.GetName())
}

func (server *Server) startTask(cli *msg2.ClientInfo, t *msg2.TaskInfo) {
	obj, err := server.findCli(cli.GetName(), cli.GetKey())
	if err != nil {
		err := fmt.Errorf("findCli(%s) err:%w", cli.GetName(), err)
		LogErrf(err.Error())
		return
	}
	r := msg2.ReqNewTask{
		Cli:  cli,
		Task: t,
	}
	reply, err := obj.TaskManagerClient.NewTask(server.ctx, &r)
	if err != nil {
		err := fmt.Errorf("NewTask(%s) by cli(%s) err:%w", t.Name, cli.GetName(), err)
		LogErrf(err.Error())
		return
	}
	_ = reply
	LogInfof("newTask (%s-%s) to init finish.", cli.GetName(), t.GetName())
}

func (server *Server) ReportState() {
	for _, cc := range server.writerClients {
		server.wg.Add(1)
		go func(c *WriterClient) {
			server.reportCliTasks(c, c.RunningTasks)
			server.wg.Done()
		}(cc)
	}
}

func (server *Server) reportCliTasks(c *WriterClient, tasks map[string]*task2.MigrateTask) {
	for _, t := range tasks {
		reply, err := c.TaskManagerClient.ReportState(server.ctx, &msg2.ReqReport{
			Cli: c.Client,
			Task: &msg2.TaskInfo{
				Name: t.Name,
				Key:  t.Key,
			},
		})
		if err != nil {
			LogErrf("ReportState(%s-%s) err:%s", c.Client.Name, t.Name, err.Error())
		} else {
			LogInfof("ReportState(%s-%s)  state(%v):%s", c.Client.Name, t.Name, reply.GetState(), reply.GetProgress())
		}
	}
}

func (server *Server) Init(taskAddress string, sec int) {

	taskListener, err := net.Listen("tcp", taskAddress)
	if err != nil {
		LogFatalf("listen on '%s' failed", taskAddress)
	}
	server.ctx, server.cancel = context.WithCancel(context.TODO())
	server.reportInterval = sec
	server.taskAddr = taskAddress
	server.writerClients = map[string]*WriterClient{}

	server.wg.Add(1)
	go server.waitTaskCli(taskListener)
	go server.handle()
	server.wg.Add(1)
	go server.reportLoop()
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
	runGrpservererver(*address)
}

func runGrpservererver(addr string) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		LogFatalf("listen err:%s", err.Error())
	}
	server := grpc.NewServer()
	msg2.RegisterToolsManagerServer(server, LightningServer)
	if err := server.Serve(listener); err != nil {
		LogFatalf("failed to serve: %v", err)
	}
}
