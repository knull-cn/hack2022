package srv

import (
	task2 "github.com/knullhhf/hack22/task"
	"testing"
	"time"
)

func TestRunning(t *testing.T) {
	go RunServer()
	cli := "dumpling-1"
	for {
		time.Sleep(time.Second)
		_, e := gcs.findCli(cli, "123456")
		if e == nil {

			break
		}
		t.Log(e)

	}
	task := task2.MigrateTask{
		ClientName: cli,
		Name:       "task-01",
		Table: &task2.TableInfo{
			Database: "middleware_pass", Name: "mcloud_middleware_env",
		},
	}
	gcs.addTask(task)
	//
	time.Sleep(1000 * time.Second)
}
