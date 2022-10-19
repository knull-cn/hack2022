package net

import (
    "context"
    "crypto/rand"
    "crypto/rsa"
    "crypto/tls"
    "crypto/x509"
    "encoding/pem"
    "fmt"
    "github.com/lucas-clemente/quic-go"
    "math/big"
    "net"
    "os"
    "time"
)

const (
    dialTimeoutSec = time.Second * 5
)

type ConnectType int

const (
    CT_TcpSocket ConnectType = iota
    CT_UnixSocket
    CT_Quic
)

type MyAddr struct {
    ct   ConnectType
    addr string
}

func (ma *MyAddr) String() string {
    switch ma.ct {
    case CT_TcpSocket:
        return fmt.Sprintf("TcpSocket-%s", ma.addr)
    case CT_UnixSocket:
        return fmt.Sprintf("UnixSocket-%s", ma.addr)
    case CT_Quic:
        return fmt.Sprintf("QuicSocket-%s", ma.addr)
    default:
        return fmt.Sprintf("%v-%s", ma.ct, ma.addr)
    }
}

type MyConn interface {
    Read(b []byte) (n int, err error)

    Write(b []byte) (n int, err error)

    Close() error

    LocalAddr() net.Addr

    RemoteAddr() net.Addr

    SetDeadline(t time.Time) error

    SetReadDeadline(t time.Time) error

    SetWriteDeadline(t time.Time) error
}

func DialByAddr(ma MyAddr, ctx context.Context) (MyConn, error) {
    switch ma.ct {
    case CT_TcpSocket:
        return net.DialTimeout("tcp", ma.addr, dialTimeoutSec)
    case CT_UnixSocket:
        return net.DialTimeout("unix", ma.addr, dialTimeoutSec)
    case CT_Quic:
        return dialQuic(ma, ctx)
    default:
        return nil, fmt.Errorf("not support type '%v(%s)'", ma.ct, ma.addr)
    }
}

type QuicConn struct {
    qc quic.Connection
    qs quic.Stream
}

func (qc *QuicConn) Read(b []byte) (n int, err error) {
    return qc.qs.Read(b)
}

func (qc *QuicConn) Write(b []byte) (n int, err error) {
    return qc.qs.Write(b)
}

func (qc *QuicConn) Close() error {
    return qc.qs.Close()
}

func (qc *QuicConn) LocalAddr() net.Addr {
    return qc.qc.LocalAddr()
}

func (qc *QuicConn) RemoteAddr() net.Addr {
    return qc.qc.RemoteAddr()
}

func (qc *QuicConn) SetDeadline(t time.Time) error {
    return qc.qs.SetDeadline(t)
}

func (qc *QuicConn) SetReadDeadline(t time.Time) error {
    return qc.qs.SetReadDeadline(t)
}

func (qc *QuicConn) SetWriteDeadline(t time.Time) error {
    return qc.qs.SetWriteDeadline(t)
}

func dialQuic(ma MyAddr, ctx context.Context) (MyConn, error) {
    tlsConf := &tls.Config{
        InsecureSkipVerify: true,
        NextProtos:         []string{"quic-echo-example"},
    }
    qc, err := quic.DialAddr(ma.addr, tlsConf, nil)
    if err != nil {
        return nil, fmt.Errorf("DialAddr %w", err)
    }
    stream, err := qc.OpenStreamSync(ctx)
    if err != nil {
        return nil, fmt.Errorf("OpenStreamSync %w", err)
    }
    return &QuicConn{qc, stream}, nil
}

type MyListener interface {
    Accept(ctx context.Context) (MyConn, error)

    Close() error

    Addr() net.Addr
}

type netListener struct {
    net.Listener
}

func (nl *netListener) Accept(ctx context.Context) (MyConn, error) {
    return nl.Listener.Accept()
}

type quicListener struct {
    quic.Listener
}

func (nl *quicListener) Accept(ctx context.Context) (MyConn, error) {
    con, err := nl.Listener.Accept(ctx)
    if err != nil {
        return nil, err
    }
    stream, err := con.AcceptStream(ctx)
    return &QuicConn{con, stream}, nil
}

func CreateListener(ma MyAddr) (MyListener, error) {
    switch ma.ct {
    case CT_TcpSocket:
        l, err := net.Listen("tcp", ma.addr)
        return &netListener{l}, err
    case CT_UnixSocket:
        os.RemoveAll(ma.addr)
        l, err := net.Listen("unix", ma.addr)
        return &netListener{l}, err
    case CT_Quic:
        l, err := quic.ListenAddr(ma.addr, generateTLSConfig(), nil)
        return &quicListener{l}, err
    default:
        return nil, fmt.Errorf("not support type '%v(%s)'", ma.ct, ma.addr)
    }
}

func generateTLSConfig() *tls.Config {
    key, err := rsa.GenerateKey(rand.Reader, 1024)
    if err != nil {
        panic(err)
    }
    template := x509.Certificate{SerialNumber: big.NewInt(1)}
    certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
    if err != nil {
        panic(err)
    }
    keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
    certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

    tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
    if err != nil {
        panic(err)
    }
    return &tls.Config{
        Certificates: []tls.Certificate{tlsCert},
        NextProtos:   []string{"quic-echo-example"},
    }
}
