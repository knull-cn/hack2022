package net

import (
	"crypto/sha256"
	"fmt"
	"gitee.com/knullhhf/hack22/net/msg"
)

func DefaultOkReplay() *msg.ReplyBase {
	return &msg.ReplyBase{
		Rc:      msg.RespCode_rc_OK,
		RespMsg: "",
	}
}

const SocketKeySize = sha256.BlockSize // use hex for key;so should *2

func SocketKey(cli, task, tkey string) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%s%s%s", cli, task, tkey)))
	v := h.Sum(nil)
	return fmt.Sprintf("%x", v)
}
