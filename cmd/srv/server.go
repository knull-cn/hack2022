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
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	mydump2 "github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/restore"
	"github.com/pingcap/tidb/util/filter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"net"
	"os"
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
		ct.RunningTasks[metas[2]].LightState = msg.TaskState_ts_Create
		ct.PendingTasks[metas[2]] = nil
		// done;
		cs.wg.Add(1)
		go cs.task4lightning(ct.RunningTasks[metas[2]], con)
	}
}

func (cs *Server) task4lightning(task *task.MigrateTask, con net.Conn) {
	defer cs.wg.Done()
	dbMetas := []*mydump2.MDDatabaseMeta{}
	for _, table := range task.Tables {
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
	}

	stauts := &restore.LightningStatus{}

	globalCfg := config.Must(config.LoadGlobalConfig([]string{"-config", "/Users/mikechengwei/Downloads/lightning/tidb-lightning.toml"}, nil))
	logToFile := globalCfg.App.File != "" && globalCfg.App.File != "-"
	if logToFile {
		fmt.Fprintf(os.Stdout, "Verbose debug logs will be written to %s\n\n", globalCfg.App.Config.File)
	}
	ctx := context.TODO()
	cfg := config.NewConfig()
	cfg.Adjust(ctx)
	if err := cfg.LoadFromGlobal(globalCfg); err != nil {
		log.L().Error("load config error")
	}

	db, err := restore.DBFromConfig(ctx, cfg.TiDB)
	if err != nil {
		log.L().Error("DB ERROR")
		return
	}
	g := glue.NewExternalTiDBGlue(db, cfg.TiDB.SQLMode)

	cfg.Checkpoint = config.Checkpoint{
		Schema: "tidb_lightning_checkpoint",
		DSN:    "/tmp/tidb_lightning_checkpoint.pb",
		Driver: "file",
		Enable: true,
	}
	cfg.App.TableConcurrency = 1
	cfg.TikvImporter.RangeConcurrency = 16
	cfg.App.IndexConcurrency = 2
	param := &restore.ControllerParam{
		DBMetas:           dbMetas,
		Status:            stauts,
		DumpFileStorage:   &storage.SocketStorage{Reader: &storage.SocketStorageReader{Connection: con}},
		OwnExtStorage:     false,
		Glue:              g,
		CheckpointStorage: nil,
		CheckpointName:    "",
	}

	procedure, err := restore.NewRestoreController(ctx, cfg, param)
	if err != nil {
		log.L().Error("restore failed", log.ShortError(err))
		return
	}
	defer procedure.Close()

	err = procedure.Run(ctx)

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

func (cs *Server) addTask(task task.MigrateTask) error {
	cli, ok := cs.writerClients[task.ClientName]
	if !ok {
		//
		return fmt.Errorf("%s not found", task.ClientName)
	}
	_, ok = cli.PendingTasks[task.Name]
	if ok {
		LogInfo("have added the task(%s) before", task.Name)
		return nil
	}

	_, ok = cli.RunningTasks[task.Name]
	if ok {
		LogInfo("the task(%s) is running", task.Name)
		return nil
	}

	_, ok = cli.FinishedTasks[task.Name]
	if ok {
		LogInfo("the task(%s) is finished", task.Name)
		return nil
	}
	cli.PendingTasks[task.Name] = &task

	return nil
}

func (cs *Server) handle() {
	for {
		for _, writer := range cs.writerClients {
			for _, task := range writer.PendingTasks {
				if task.DumpState != msg.TaskState_ts_Create {
					cs.newTask(&writer.Client, msg.TaskInfo{
						TaskName: task.Name,
						TaskKey:  task.TaskKey,
						Db:       task.Table.Database,
						Tbl:      task.Table.Name,
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
	gc, err := grpc.DialContext(cs.ctx, in.GetTaskAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		err := fmt.Errorf("dial context task addr(%s) err:%w", in.GetTaskAddr(), err)
		LogErr(err.Error())
		return nil, err
	}
	//
	cs.cliMtx.Lock()
	cs.writerClients[in.Cli.Name] = &cli.WriterClient{
		Client:            *client,
		TaskManagerClient: msg.NewTaskManagerClient(gc),
		PendingTasks:      map[string]*task.MigrateTask{},
		FinishedTasks:     map[string]*task.MigrateTask{},
		RunningTasks:      map[string]*task.MigrateTask{},
	}
	cs.cliMtx.Unlock()
	LogInfo("new tool(%s) registered", client.GetName())
	return &msg.ReplyRegister{Rc: mnet.DefaultOkReplay()}, nil
}

func (cs *Server) newTask(cli *msg.ClientInfo, t msg.TaskInfo) {
	obj, err := cs.findCli(cli.GetName(), cli.GetKey())
	if err != nil {
		err := fmt.Errorf("findCli(%s) err:%w", cli.GetName(), err)
		LogErr(err.Error())
		return
	}
	r := msg.ReqNewTask{
		Cli:      cli,
		Task:     &t,
		TaskAddr: cs.taskAddr,
	}
	reply, err := obj.TaskManagerClient.NewTask(cs.ctx, &r)
	if err != nil {
		err := fmt.Errorf("NewTask(%s) by cli(%s) err:%w", t.TaskName, cli.GetName(), err)
		LogErr(err.Error())
		return
	}
	_ = reply
	LogInfo("newTask (%s-%s) to init finish.", cli.GetName(), t.GetTaskName())
}

func (cs *Server) startTask(cli *msg.ClientInfo, t msg.TaskInfo) {
	obj, err := cs.findCli(cli.GetName(), cli.GetKey())
	if err != nil {
		err := fmt.Errorf("findCli(%s) err:%w", cli.GetName(), err)
		LogErr(err.Error())
		return
	}
	r := msg.ReqNewTask{
		Cli:      cli,
		Task:     &t,
		TaskAddr: cs.taskAddr,
	}
	reply, err := obj.TaskManagerClient.NewTask(cs.ctx, &r)
	if err != nil {
		err := fmt.Errorf("NewTask(%s) by cli(%s) err:%w", t.TaskName, cli.GetName(), err)
		LogErr(err.Error())
		return
	}
	_ = reply
	LogInfo("newTask (%s-%s) to init finish.", cli.GetName(), t.GetTaskName())
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
			Cli: &c.Client,
			Task: &msg.TaskInfo{
				TaskName: t.Name,
				TaskKey:  t.TaskKey,
			},
		})
		if err != nil {
			LogErr("ReportState(%s-%s) err:%s", c.Client.Name, t.Name, err.Error())
		} else {
			LogInfo("ReportState(%s-%s)  state(%v):%s", c.Client.Name, t.Name, reply.GetState(), reply.GetProgress())
		}
	}
}

func (cs *Server) Init(taskAddr string, sec int) {

	taskListner, err := net.Listen("tcp", taskAddr)
	if err != nil {
		LogFatal("listen on '%s' failed", taskAddr)
	}
	cs.ctx, cs.cancel = context.WithCancel(context.TODO())
	cs.reportInterval = sec
	cs.taskAddr = taskAddr
	cs.writerClients = map[string]*cli.WriterClient{}

	cs.wg.Add(1)
	go cs.waitTaskCli(taskListner)
	go cs.handle()
	cs.wg.Add(1)
	go cs.reportLoop()
}

var (
	gcs = &Server{}
	//
	saddr    = flag.String("l", ":9876", "address of listener")
	taskAddr = flag.String("t", ":9877", "listener of task ")
	report   = flag.Int("r", 1, "report of interval second")
)

func RunServer() {
	flag.Parse()
	gcs.Init(*taskAddr, 1)
	runGrpcServer(*saddr)
}

func runGrpcServer(addr string) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		LogFatal("listen err:%s", err.Error())
	}
	server := grpc.NewServer()
	msg.RegisterToolsManagerServer(server, gcs)
	if err := server.Serve(lis); err != nil {
		LogFatal("failed to serve: %v", err)
	}
}
