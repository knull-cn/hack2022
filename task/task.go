package task

import "github.com/knullhhf/hack22/net/msg"

type MigrateTask struct {
	ClientName    string
	Name          string
	TaskKey       string
	Table         *TableInfo
	DumpState     msg.TaskState
	DumpProgress  string
	LightState    msg.TaskState // TODO :for lightning state;
	LightProgress string
}

type TableInfo struct {
	Database string
	Name     string
}
