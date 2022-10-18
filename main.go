package main

import (
	"github.com/beego/beego/v2/core/logs"
	beego "github.com/beego/beego/v2/server/web"
	"github.com/knullhhf/hack22/logger"
	"github.com/knullhhf/hack22/pkg/server"
	"github.com/knullhhf/hack22/repo"
	_ "github.com/knullhhf/hack22/routers"
)

func main() {
	logger.InitLog()

	err := repo.InitDB()
	if err != nil {
		logs.Error("init db error:%v", err)
	}

	go server.RunServer()
	beego.Run(":8080")
}
