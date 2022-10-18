package client

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

func DialByAddr(ma MyAddr) (MyConn, error) {
    switch ma.ct {
    case CT_TcpSocket:
        return net.DialTimeout("tcp", ma.addr, dialTimeoutSec)
    case CT_UnixSocket:
        return net.DialTimeout("unix", ma.addr, dialTimeoutSec)
    case CT_Quic:
        return dialQuic(ma)
    default:
        return nil, fmt.Errorf("not support type '%v(%s)'", ma.ct, ma.addr)
    }
}

type QuicConn struct {
    quic.Connection
    quic.Stream
}

func dialQuic(ma MyAddr) (MyConn, error) {
    tlsConf := &tls.Config{
        InsecureSkipVerify: true,
        NextProtos:         []string{"quic-echo-example"},
    }
    qc, err := quic.DialAddr(ma.addr, tlsConf, nil)
    if err != nil {
        return nil, err
    }
    stream, err := qc.OpenStreamSync(context.TODO())
    if err != nil {
        return nil, err
    }
    return &QuicConn{qc, stream}, nil
}

type MyListener interface {
    Accept() (MyConn, error)

    Close() error

    Addr() net.Addr
}

type netListener struct {
    net.Listener
}

func (nl *netListener) Accept() (MyConn, error) {
    return nl.Listener.Accept()
}

type quicListener struct {
    quic.Listener
}

func (nl *quicListener) Accept() (MyConn, error) {
    con, err := nl.Listener.Accept(context.TODO())
    if err != nil {
        return nil, err
    }
    stream, err := con.AcceptStream(context.TODO())
    return &QuicConn{con, stream}, nil
}

func CreateListener(ma MyAddr) (MyListener, error) {
    switch ma.ct {
    case CT_TcpSocket:
        l, err := net.Listen("tcp", ma.addr)
        return &netListener{l}, err
    case CT_UnixSocket:
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
