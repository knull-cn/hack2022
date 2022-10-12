package task

import (
	"github.com/knullhhf/hack22/net/msg"
	"github.com/knullhhf/hack22/net/storage"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
)

type MigrateTask struct {
	ClientName string
	Name       string
	TaskKey    string

	Table         *TableInfo
	DumpState     msg.TaskState
	DumpProgress  string
	LightState    msg.TaskState // TODO :for lightning state;
	LightProgress string

	Config       *config.Config
	WriterSource *storage.SocketStorage
}

type TableInfo struct {
	Database string
	Name     string
}
