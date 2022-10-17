package server

import (
	task2 "github.com/knullhhf/hack22/pkg/models/task"
	msg2 "github.com/knullhhf/hack22/pkg/net/msg"
)

type WriterClient struct {
	Client            *msg2.ClientInfo
	AuthKey           string
	TaskManagerClient msg2.TaskManagerClient
	PendingTasks      map[string]*task2.MigrateTask
	RunningTasks      map[string]*task2.MigrateTask
	FinishedTasks     map[string]*task2.MigrateTask
}
