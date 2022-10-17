package datasource

import (
	"github.com/knullhhf/hack22/common"
	"github.com/knullhhf/hack22/controller"
)

type DataSourceController struct {
	controller.BaseController
}

func (d *DataSourceController) Post() {
	switch d.Ctx.Request.Header.Get("action") {
	case "ListDataSource":
		d.ListDataSource()
	default:
		d.ErrorResp("action不支持", common.APICodeNotFoundPath, common.Newf("动作不支持"))
	}
}

func (d *DataSourceController) ListDataSource() {

}

func (d *DataSourceController) ListDataBase() {

}
