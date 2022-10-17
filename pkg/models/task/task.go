package task

import (
	msg2 "github.com/knullhhf/hack22/pkg/net/msg"
	storage2 "github.com/knullhhf/hack22/pkg/net/storage"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
)

type MigrateTask struct {
	ClientName string
	Name       string
	Key        string

	Source        *TableInfo
	Target        *TableInfo
	DumpState     msg2.TaskState
	DumpProgress  string
	LightState    msg2.TaskState // TODO :for lightning state;
	LightProgress string

	Config       *config.Config
	WriterSource *storage2.SocketStorage
}

type TableInfo struct {
	Host     string
	Port     int32
	Username string
	Password string
	Database string
	Name     string
}
