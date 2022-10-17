package routers

import (
	beego "github.com/beego/beego/v2/server/web"
	"github.com/beego/beego/v2/server/web/context"
	"github.com/knullhhf/hack22/controller/client"
	"github.com/knullhhf/hack22/controller/datasource"
	"github.com/knullhhf/hack22/controller/task"
)

func init() {
	ns := beego.NewNamespace("/api/v1",
		beego.NSRouter("/client", &client.ClientController{}, "post:Post"),
		beego.NSRouter("/task", &task.TaskController{}, "post:Post"),
		beego.NSRouter("/datasource", &datasource.DataSourceController{}, "post:Post"),
	)
	beego.AddNamespace(ns)
	beego.Get("/health", func(ctx *context.Context) {
		ctx.Output.Body([]byte("pong"))
	})
}
