package net

import (
	"fmt"
	msg2 "github.com/knullhhf/hack22/pkg/net/msg"
)

func DefaultOkReplay() *msg2.ReplyBase {
	return &msg2.ReplyBase{
		Rc:      msg2.RespCode_rc_OK,
		RespMsg: "",
	}
}

func SocketKey(clientName, task string) string {
	//h := sha256.New()
	//h.Write([]byte(fmt.Sprintf("task:%s:%s", clientName, task)))
	//v := h.Sum(nil)
	return fmt.Sprintf("task:%s:%s", clientName, task)
}
