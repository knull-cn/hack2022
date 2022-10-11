package storage

import (
	"context"
	"github.com/pingcap/errors"
	"net"

	br "github.com/pingcap/tidb/br/pkg/storage"
)

type SocketStorage struct {
	Reader *SocketStorageReader
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
	panic("implement me")
}

func (l *SocketStorage) Rename(ctx context.Context, oldFileName, newFileName string) error {
	panic("implement me")
}

type SocketStorageReader struct {
	Connection net.Conn
}

func (s SocketStorageReader) Read(p []byte) (n int, err error) {
	n, err = s.Connection.Read(p)
	return n, err
}

func (s SocketStorageReader) Close() error {
	return s.Connection.Close()
}

func (s SocketStorageReader) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}
