package storage

import (
	"context"
	"github.com/knullhhf/hack22/logger"
	"github.com/knullhhf/hack22/net/msg"
	"github.com/pingcap/errors"
	"net"
	"strings"

	br "github.com/pingcap/tidb/br/pkg/storage"
)

type SocketStorage struct {
	Reader *SocketStorageReader
	Writer *SocketStorageWriter
}

func (l *SocketStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	panic("implement me")

}

func (l *SocketStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	panic("implement me")
}

func (l *SocketStorage) FileExists(ctx context.Context, name string) (bool, error) {
	panic("implement me")
}

func (l *SocketStorage) DeleteFile(ctx context.Context, name string) error {
	panic("implement me")
}

func (l *SocketStorage) Open(ctx context.Context, path string) (br.ExternalFileReader, error) {
	return l.Reader, nil
}

func (l *SocketStorage) WalkDir(ctx context.Context, opt *br.WalkOption, fn func(path string, size int64) error) error {
	if opt != nil && opt.ListCount == 1 {
		return errors.New("Stop Iter")
	} else {
		return nil
	}

}

func (l *SocketStorage) URI() string {
	return "socket:///"
}

func (l *SocketStorage) Create(ctx context.Context, path string) (br.ExternalFileWriter, error) {
	if strings.Contains(path, "metadata") {
		b := true
		l.Writer.NotTransferData = &b
	}
	return l.Writer, nil
}

func (l *SocketStorage) Rename(ctx context.Context, oldFileName, newFileName string) error {
	panic("implement me")
}

type SocketStorageWriter struct {
	Connection      net.Conn
	NotTransferData *bool
}

func (u *SocketStorageWriter) Write(ctx context.Context, data []byte) (int, error) {
	if (u.NotTransferData != nil) && *u.NotTransferData {
		return 0, nil
	}
	return u.Connection.Write(data)
}

func (u *SocketStorageWriter) Close(ctx context.Context) error {
	return nil
	//return u.Connection.Close()
}

type SocketStorageReader struct {
	Connection        net.Conn
	TaskManagerClient msg.TaskManagerClient
	TaskInfo          *msg.ReqNewTask
	IsReadHeader      *bool
	FirstDataRead     *bool
}

func (s *SocketStorageReader) Read(p []byte) (n int, err error) {
	//if s.IsReadHeader == nil {
	//
	//	b := true
	//	s.IsReadHeader = &b
	//
	//	return 0, nil
	//}
	ctx := context.TODO()
	// start task write
	if s.FirstDataRead == nil || !*s.FirstDataRead {
		_, err = s.TaskManagerClient.StartTask(ctx, s.TaskInfo)
		if err != nil {
			logger.LogErr("start task error:%v", err)
		}
		b := true
		s.FirstDataRead = &b
	}

	n, err = s.Connection.Read(p)
	return n, err
}

func (s *SocketStorageReader) Close() error {
	//return s.Connection.Close()
	return nil
}

func (s *SocketStorageReader) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}
