package srv

import (
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

	task := srvTask{
		name:    "task-01",
		taskKey: "1234",
	}
	gcs.addTask(cli, task)
	//
	time.Sleep(1000 * time.Second)
}
