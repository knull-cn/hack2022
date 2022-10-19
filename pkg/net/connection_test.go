package net

import (
    "context"
    "fmt"
    "io"
    "sync"
    "sync/atomic"
    "testing"
    "time"
)

var (
    testAddrList = []MyAddr{
        {CT_TcpSocket, ":9876"},
        {CT_UnixSocket, "/tmp/my9876"},
        {CT_Quic, "localhost:9876"},
    }
    wg        = &sync.WaitGroup{}
    idx int64 = 0
)

func runServer(t *testing.T, ctx context.Context) {
    wg.Add(3)
    for _, addr := range testAddrList {
        l, err := CreateListener(addr)
        if err != nil {
            t.Fatalf("CreateListener(%s)err:%s", addr.String(), err.Error())
        }
        t.Logf("CreateListener(%s) ok", addr.String())
        go func() {
            process(t, l, ctx)
            wg.Done()
        }()
    }
    time.Sleep(time.Second)
}

func process(t *testing.T, listener MyListener, ctx context.Context) {
    var buf [1024]byte
    running := true
    for running {
        select {
        case <-ctx.Done():
            t.Logf("%s exiting", listener.Addr().String())
            running = false
            break
        default:
            con, err := listener.Accept(ctx)
            if err != nil {
                t.Errorf("%s accept err:%s", listener.Addr().String(), err.Error())
                continue
            }
            n, err := con.Read(buf[:])
            t.Logf("read from %s : %s.", con.RemoteAddr().String(), string(buf[:n]))
            con.Write([]byte("bye"))
            con.Close()
        }
    }
}

func runClient(t *testing.T, ctx context.Context, ma MyAddr) {
    t.Logf("test '%s'", ma.String())
    con, err := DialByAddr(ma, ctx)
    if err != nil {
        t.Errorf("DialByAddr(%s) err:%s.", ma.String(), err.Error())
        return
    }
    // write and read;
    data := fmt.Sprintf("from '%s' write '%d'", ma.String(), atomic.AddInt64(&idx, 1))
    con.Write([]byte(data))
    var buf [1024]byte
    n, err := con.Read(buf[:])
    con.Close()
    if err == io.EOF {
        t.Logf("%s : recive '%s' and close", ma.String(), string(buf[:n]))
    } else {
        t.Logf("%s : recive '%s' ", ma.String(), string(buf[:n]))
    }

}

func TestConnection(t *testing.T) {
    ctx, cancel := context.WithCancel(context.TODO())
    runServer(t, ctx)
    for _, addr := range testAddrList {
        runClient(t, ctx, addr)
    }
    cancel()
    wg.Wait()
}
