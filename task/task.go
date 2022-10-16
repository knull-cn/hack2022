package task

import (
	"github.com/knullhhf/hack22/net/msg"
	"github.com/knullhhf/hack22/net/storage"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
)

type MigrateTask struct {
	ClientName string
	Name       string
	Key        string

	Source        *TableInfo
	Target        *TableInfo
	DumpState     msg.TaskState
	DumpProgress  string
	LightState    msg.TaskState // TODO :for lightning state;
	LightProgress string

	Config       *config.Config
	WriterSource *storage.SocketStorage
}

type TableInfo struct {
	Host     string
	Port     int32
	Username string
	Password string
	Database string
	Name     string
}
