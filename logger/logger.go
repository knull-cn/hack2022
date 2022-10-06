package logger

import (
	"encoding/json"
	"fmt"
	"log"
)

func LogErr(format string, v ...interface{}) {
	s := fmt.Sprintf(format, v...)
	log.Output(2, "[ ERR ] "+s)
}

func LogWarn(format string, v ...interface{}) {
	s := fmt.Sprintf(format, v...)
	log.Output(2, "[ Warn] "+s)
}

func LogInfo(format string, v ...interface{}) {
	s := fmt.Sprintf(format, v...)
	log.Output(2, "[ INFO] "+s)
}

func LogTraceJson(format string, v interface{}) {
	d, _ := json.Marshal(v)
	s := fmt.Sprintf(format, string(d))
	log.Output(2, "[Trace] "+s)
}

func LogTrace(format string, v ...interface{}) {
	s := fmt.Sprintf(format, v...)
	log.Output(2, "[Trace] "+s)
}

func LogFatal(format string, v ...interface{}) {
	s := fmt.Sprintf(format, v...)
	log.Output(2, "[Fatal] "+s)
	log.Println("")
	panic(nil)
}
