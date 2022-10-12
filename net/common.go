package net

import (
	"fmt"
	"github.com/knullhhf/hack22/net/msg"
)

func DefaultOkReplay() *msg.ReplyBase {
	return &msg.ReplyBase{
		Rc:      msg.RespCode_rc_OK,
		RespMsg: "",
	}
}

func SocketKey(clientName, task string) string {
	//h := sha256.New()
	//h.Write([]byte(fmt.Sprintf("task:%s:%s", clientName, task)))
	//v := h.Sum(nil)
	return fmt.Sprintf("task:%s:%s", clientName, task)
}
