package client

import (
	"github.com/knullhhf/hack22/common"
	"github.com/knullhhf/hack22/controller"
	"github.com/knullhhf/hack22/pkg/models/client"
	"github.com/knullhhf/hack22/pkg/server"
	"github.com/knullhhf/hack22/pkg/utils/mapper"
)

type ClientController struct {
	controller.BaseController
}

func (c *ClientController) Post() {
	switch c.Ctx.Request.Header.Get("action") {
	case "ListClient":
		c.ListClient()
	default:
		c.ErrorResp("action不支持", common.APICodeNotFoundPath, common.Newf("动作不支持"))
	}
}

func (c *ClientController) ListClient() {
	clients := server.LightningServer.ListWriteClient()

	var clientDtos []*client.ClientDto
	mapper.Copy(&clientDtos, &clients)
	c.SuccessResp(clientDtos)
}
