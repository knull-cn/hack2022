package srv

import (
	"context"
	"fmt"
	task2 "github.com/knullhhf/hack22/task"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"os"
	"testing"
	"time"
)

func TestRunning(t *testing.T) {
	go RunServer()
	cli := "dumpling-1"
	for {
		time.Sleep(time.Second)
		_, e := LightningServer.findCli(cli, "123456")
		if e == nil {

			break
		}
		t.Log(e)

	}
	// generate task config
	globalCfg := config.Must(config.LoadGlobalConfig([]string{"-config", "/Users/mikechengwei/go-project/src/github.com/hack2022/config/tidb-lightning.toml"}, nil))
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
	cfg.Checkpoint = config.Checkpoint{
		Schema: "tidb_lightning_checkpoint",
		DSN:    "/tmp/tidb_lightning_checkpoint.pb",
		Driver: "file",
		Enable: true,
	}
	cfg.App.TableConcurrency = 1
	cfg.TikvImporter.RangeConcurrency = 16
	cfg.App.IndexConcurrency = 2
	cfg.Mydumper.CSV.Header = true
	cfg.Mydumper.CSV.Header = true
	cfg.App.CheckRequirements = false

	task := task2.MigrateTask{
		ClientName: cli,
		Name:       "task-01",
		Key:        "task-01",
		Source: &task2.TableInfo{
			Host:     "127.0.0.1",
			Port:     3306,
			Username: "root",
			Password: "12345678",
			Database: "middleware_pass", Name: "mcloud_middleware_env",
		},
		Target: &task2.TableInfo{
			Database: "middleware_pass", Name: "mcloud_middleware_env",
		},
		Config: cfg,
	}

	task2 := task2.MigrateTask{
		ClientName: cli,
		Name:       "task-02",
		Key:        "task-02",
		Source: &task2.TableInfo{
			Host:     "127.0.0.1",
			Port:     3306,
			Username: "root",
			Password: "12345678",
			Database: "middleware_pass", Name: "mcloud_middleware_control_info",
		},
		Target: &task2.TableInfo{
			Database: "middleware_pass", Name: "mcloud_middleware_control_info",
		},
		Config: cfg,
	}

	// create task
	LightningServer.addTask(&task)
	LightningServer.addTask(&task2)
	time.Sleep(1000000 * time.Second)
}
