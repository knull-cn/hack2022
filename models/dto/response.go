package dto

import (
	"github.com/knullhhf/hack22/common"
)

type Response struct {
	Code    common.Code `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}
