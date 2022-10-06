package srv

import (
	"context"
	"flag"
	"fmt"
	. "gitee.com/knullhhf/hack22/logger"
	mnet "gitee.com/knullhhf/hack22/net"
	"gitee.com/knullhhf/hack22/net/msg"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type CtlSrv struct {
	msg.UnimplementedToolsManagerServer
	//
	addr     string
	taskAddr string
	//
	cliMtx sync.Mutex
	clis   map[string]*ctlCliObj

	//
	taskMtx        sync.Mutex
	taskPending    map[string]pendingTask
	reportInterval int // second
	//
	ctx    context.Context
	cancel context.CancelFunc
	//
	wg sync.WaitGroup
}

type ctlCliObj struct {
	cli   msg.ClientInfo
	tc    msg.TaskManagerClient
	tasks map[string]*srvTask
}

type pendingTask struct {
	cli string
	t   srvTask
}

type srvTask struct {
	name          string
	taskKey       string
	db, tbl       string
	dumpState     msg.TaskState
	dumpProgress  string
	lightState    msg.TaskState // TODO :for lightning state;
	lightProgress string
}

func (cs *CtlSrv) exit() {
	cs.cancel()
}

func (cs *CtlSrv) waitTaskCli(lis net.Listener) {
	LogInfo("waitTaskCli running")
	defer cs.wg.Done()
	for {
		con, err := lis.Accept()
		if err != nil {
			LogErr("accept err:%s", err.Error())
			cs.exit()
		}
		// auth;
		var buf [mnet.SocketKeySize]byte
		n, err := io.LimitReader(con, mnet.SocketKeySize).Read(buf[:])
		if err != nil {
			LogErr("accept err:%s", err.Error())
			return
		}

		if n < mnet.SocketKeySize {
			continue
		}
		key := string(buf[:mnet.SocketKeySize])
		cs.taskMtx.Lock()
		ct, ok := cs.taskPending[key]
		if !ok {
			cs.taskMtx.Unlock()
			LogErr("not found key(%s) in pending", key)
			return
		}
		delete(cs.taskPending, key)
		cs.taskMtx.Unlock()
		// change state to task;
		ct.t.dumpState = msg.TaskState_ts_Create
		ct.t.lightState = msg.TaskState_ts_Create
		obj := cs.clis[ct.cli]
		obj.tasks[ct.t.name] = &ct.t
		ctstr := fmt.Sprintf("%s-%s", ct.cli, ct.t.name)
		LogInfo("wait task(%s) and auth ok and running", ctstr)
		// done;
		cs.wg.Add(1)
		go cs.task4lightning(ctstr, con)
	}
}

func (cs *CtlSrv) task4lightning(ct string, con net.Conn) {
	defer cs.wg.Done()
	// TODO: connect to lightning;

	// print for test;
	var buf [1024]byte
	for {
		n, err := con.Read(buf[:])
		if err != nil {
			if err == io.EOF {
				if n > 0 {
					LogInfo("task4lightning(%s) ---> %s", ct, string(buf[:n]))
				}
				break
			} else {
				LogErr("read(%s) err:%s", ct, err.Error())
				cs.exit()
				return
			}
		}
		LogInfo("task4lightning(%s) ---> %s", ct, string(buf[:n]))
	}
}

func (cs *CtlSrv) reportLoop() {
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

func (cs *CtlSrv) addTask(name string, task srvTask) error {
	lcli, ok := cs.clis[name]
	if !ok {
		return fmt.Errorf("not found")
	}
	_, ok = lcli.tasks[task.name]
	if ok {
		// has added;
		LogInfo("running the task(%s) before", task.name)
		return nil
	}
	// auth key;
	ak := mnet.SocketKey(name, task.name, task.taskKey)
	_, ok = cs.taskPending[ak]
	if ok {
		// has added;
		LogInfo("has insert the task(%s) before", task.name)
		return nil
	}
	cs.taskPending[ak] = pendingTask{
		cli: name,
		t:   task,
	}
	LogInfo("add task (%s-%s)(%s) to pending...", name, task.name, ak)
	cs.newTask(&lcli.cli, msg.TaskInfo{
		TaskName: task.name,
		TaskKey:  task.taskKey,
		Db:       task.db,
		Tbl:      task.tbl,
	})

	return nil
}

func (cs *CtlSrv) findCli(name, key string) (*ctlCliObj, error) {
	var skey string
	cs.cliMtx.Lock()
	lcli, ok := cs.clis[name]
	if lcli != nil {
		skey = lcli.cli.GetKey()
	}
	cs.cliMtx.Unlock()
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	if skey != key {
		return nil, fmt.Errorf("auth failed")
	}
	return lcli, nil
}

func (cs *CtlSrv) Register(ctx context.Context, in *msg.ReqRegister) (*msg.ReplyRegister, error) {
	cli := in.Cli
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
	cs.clis[cli.Name] = &ctlCliObj{
		cli:   *cli,
		tc:    msg.NewTaskManagerClient(gc),
		tasks: map[string]*srvTask{},
	}
	cs.cliMtx.Unlock()
	LogInfo("new tool(%s) registered", cli.GetName())
	return &msg.ReplyRegister{Rc: mnet.DefaultOkReplay()}, nil
}

func (cs *CtlSrv) newTask(cli *msg.ClientInfo, t msg.TaskInfo) {
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
	reply, err := obj.tc.NewTask(cs.ctx, &r)
	if err != nil {
		err := fmt.Errorf("NewTask(%s) by cli(%s) err:%w", t.TaskName, cli.GetName(), err)
		LogErr(err.Error())
		return
	}
	_ = reply
	LogInfo("newTask (%s-%s) to init finish.", cli.GetName(), t.GetTaskName())
}

func (cs *CtlSrv) ReportState() {
	for _, cc := range cs.clis {
		cs.wg.Add(1)
		go func(c *ctlCliObj) {
			cs.reportCliTasks(c)
			cs.wg.Done()
		}(cc)
	}
}

func (cs *CtlSrv) reportCliTasks(c *ctlCliObj) {
	for _, t := range c.tasks {
		reply, err := c.tc.ReportState(cs.ctx, &msg.ReqReport{
			Cli: &c.cli,
			Task: &msg.TaskInfo{
				TaskName: t.name,
				TaskKey:  t.taskKey,
			},
		})
		if err != nil {
			LogErr("ReportState(%s-%s) err:%s", c.cli.Name, t.name, err.Error())
		} else {
			LogInfo("ReportState(%s-%s)  state(%v):%s", c.cli.Name, t.name, reply.GetState(), reply.GetProgress())
		}
	}
}

func (cs *CtlSrv) Init(taskAddr string, sec int) {

	lis2, err := net.Listen("tcp", taskAddr)
	if err != nil {
		log.Fatalf("listen on '%s' failed", taskAddr)
	}
	cs.ctx, cs.cancel = context.WithCancel(context.TODO())
	cs.reportInterval = sec
	cs.taskAddr = taskAddr

	//
	cs.clis = map[string]*ctlCliObj{}
	cs.taskPending = map[string]pendingTask{}

	cs.wg.Add(1)
	go cs.waitTaskCli(lis2)
	cs.wg.Add(1)
	go cs.reportLoop()
}

var (
	gcs = &CtlSrv{}
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
	gs := grpc.NewServer()
	msg.RegisterToolsManagerServer(gs, gcs)
	gs.Serve(lis)
}
