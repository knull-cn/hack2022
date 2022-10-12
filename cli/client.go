package cli

import (
	"github.com/knullhhf/hack22/net/msg"
	"github.com/knullhhf/hack22/task"
)

type WriterClient struct {
	Client            *msg.ClientInfo
	AuthKey           string
	TaskManagerClient msg.TaskManagerClient
	PendingTasks      map[string]*task.MigrateTask
	RunningTasks      map[string]*task.MigrateTask
	FinishedTasks     map[string]*task.MigrateTask
}
