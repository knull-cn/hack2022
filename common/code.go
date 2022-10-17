package common

import "fmt"

type Code int32

type CommonError struct {
	Code   Code  `json:"code"`
	Detail error `json:"detail"`
}

func (e CommonError) Error() string {
	return e.Detail.Error()
}

const (
	APISuccess          Code = 0
	APISystemError      Code = 10000
	APICodeNotFoundPath Code = 10001
)

const (
	DbError     Code = 00100
	SYSTEMERROR Code = 00101
)

func IsInternalCode(code Code) bool {
	return code < APISystemError
}

func Newf(msg string, args ...interface{}) error {
	return CommonError{
		Code:   SYSTEMERROR,
		Detail: fmt.Errorf(msg, args...),
	}
}
