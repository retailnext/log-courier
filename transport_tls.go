package main

import (
  "bytes"
  "crypto/tls"
  "crypto/x509"
  "encoding/binary"
  "encoding/pem"
  "errors"
  "fmt"
  "io/ioutil"
  "log"
  "math/rand"
  "net"
  "regexp"
  "sync"
  "time"
)

// Support for newer SSL signature algorithms
import _ "crypto/sha256"
import _ "crypto/sha512"

type TransportTls struct {
  config      *NetworkConfig
  tls_config  tls.Config
  socket      *tls.Conn
  hostport_re *regexp.Regexp

  write_buffer *bytes.Buffer

  wait sync.WaitGroup
  shutdown chan int

  send_chan chan []byte
  recv_chan chan interface{}

  can_send chan int
  can_recv chan int
}

// If tls.Conn.Write ever times out it will permanently break, so we cannot use SetWriteDeadline with it directly
// So we wrap the given tcpsocket and handle the SetWriteDeadline there and check shutdown signal and loop
// Inside tls.Conn the Write blocks until it finishes and everyone is happy
type TransportTlsWrap struct {
  transport *TransportTls
  tcpsocket net.Conn

  net.Conn
}

const (
  socket_interval_seconds = 5
)

func CreateTransportTls(config *NetworkConfig) (*TransportTls, error) {
  rand.Seed(time.Now().UnixNano())

  ret := &TransportTls{
    config: config,
    hostport_re: regexp.MustCompile(`^\[?([^]]+)\]?:([0-9]+)$`),
    write_buffer: new(bytes.Buffer),
  }

  if len(config.SSLCertificate) > 0 && len(config.SSLKey) > 0 {
    log.Printf("Loading client ssl certificate: %s and %s\n", config.SSLCertificate, config.SSLKey)
    cert, err := tls.LoadX509KeyPair(config.SSLCertificate, config.SSLKey)
    if err != nil {
      return nil, errors.New(fmt.Sprintf("Failed loading client ssl certificate: %s", err))
    }
    ret.tls_config.Certificates = []tls.Certificate{cert}
  }

  if len(config.SSLCA) > 0 {
    log.Printf("Setting trusted CA from file: %s\n", config.SSLCA)
    ret.tls_config.RootCAs = x509.NewCertPool()

    pemdata, err := ioutil.ReadFile(config.SSLCA)
    if err != nil {
      return nil, errors.New(fmt.Sprintf("Failure reading CA certificate: %s", err))
    }

    block, _ := pem.Decode(pemdata)
    if block == nil {
      return nil, errors.New("Failed to decode CA certificate data")
    }
    if block.Type != "CERTIFICATE" {
      return nil, errors.New(fmt.Sprintf("Specified CA certificate is not a certificate: %s", config.SSLCA))
    }

    cert, err := x509.ParseCertificate(block.Bytes)
    if err != nil {
      return nil, errors.New(fmt.Sprintf("Failed to parse CA certificate: %s", err))
    }
    ret.tls_config.RootCAs.AddCert(cert)
  }

  return ret, nil
}

func (t *TransportTls) Connect() error {
  t.write_buffer = new(bytes.Buffer)

Connect:
  for {
    for {
      // Pick a random server from the list.
      hostport := t.config.Servers[rand.Int()%len(t.config.Servers)]
      submatch := t.hostport_re.FindSubmatch([]byte(hostport))
      if submatch == nil {
        log.Printf("Invalid host:port given: %s\n", hostport)
        break
      }

      // Lookup the server in DNS (if this is IP it will implicitly return)
      host := string(submatch[1])
      port := string(submatch[2])
      addresses, err := net.LookupHost(host)
      if err != nil {
        log.Printf("DNS lookup failure \"%s\": %s\n", host, err)
        break
      }

      // Select a random address from the pool of addresses provided by DNS
      address := addresses[rand.Int()%len(addresses)]
      addressport := net.JoinHostPort(address, port)

      log.Printf("Connecting to %s (%s) \n", addressport, host)

      tcpsocket, err := net.DialTimeout("tcp", addressport, t.config.timeout)
      if err != nil {
        log.Printf("Failure connecting to %s: %s\n", address, err)
        break
      }

      t.socket = tls.Client(&TransportTlsWrap{transport: t, tcpsocket: tcpsocket}, &t.tls_config)
      t.socket.SetDeadline(time.Now().Add(t.config.timeout))
      err = t.socket.Handshake()
      if err != nil {
        t.socket.Close()
        log.Printf("TLS Handshake failure with %s: %s\n", address, err)
        break
      }

      log.Printf("Connected with %s\n", address)

      // Connected, let's rock and roll.
      break Connect

    } /* for, break for sleep */

    time.Sleep(t.config.reconnect)
  } /* Connect: for */

  // Signal channels
  t.shutdown = make(chan int, 1)
  t.send_chan = make(chan []byte, 1)
  t.recv_chan = make(chan interface{}, 1)
  t.can_send = make(chan int, 1)
  t.can_recv = make(chan int, 1)

  // Start with a send
  t.can_send <- 1

  t.wait.Add(2)

  // Start separate sender and receiver so we can asynchronously send and receive for max performance
  // They have to be different routines too because we don't have cross-platform poll, so they will need to block
  // Of course, we'll time out and check shutdown on occasion
  go t.sender()
  go t.receiver()

  return nil
}

func (t *TransportTls) sender() {
SendLoop:
  for {
    select {
      case <-t.shutdown:
        // Shutdown
        break SendLoop
      case msg := <-t.send_chan:
        // Write deadline is managed by our net.Conn wrapper that tls will call into
        _, err := t.socket.Write(msg)
        if err == nil {
          t.setChan(t.can_send)
        } else if net_err, ok := err.(net.Error); ok && net_err.Timeout() {
          // Shutdown will have been received by the wrapper
          break SendLoop
        } else {
          // Pass error back
          t.recv_chan <- err
          t.setChan(t.can_recv)
        }
    }
  }

  t.wait.Done()
}

func (t *TransportTls) receiver() {
  var err error
  var shutdown bool
  header := make([]byte, 8)

  for {
    if err, shutdown = t.receiverRead(header); err != nil || shutdown {
      break
    }

    // Grab length of message
    length := binary.BigEndian.Uint32(header[4:8])

    // Sanity
    if length > 1048576 {
      t.recv_chan <- errors.New(fmt.Sprintf("Received message too large (%d)", length))
    }

    // Allocate for full message including header
    message := make([]byte, 8 + length)
    copy(message, header)

    if err, shutdown = t.receiverRead(message[8:]); err != nil || shutdown {
      break
    }

    // Pass back the message
    t.recv_chan <- message
    t.setChan(t.can_recv)
  } /* loop until shutdown */

  if err != nil {
    // Pass the error back and abort
    t.recv_chan <- err
    t.setChan(t.can_recv)
  }

  t.wait.Done()
}

func (t *TransportTls) receiverRead(data []byte) (error, bool) {
  received := 0

RecvLoop:
  for {
    select {
    case <-t.shutdown:
      // Shutdown
      break RecvLoop
    default:
      // Timeout after socket_interval_seconds, check for shutdown, and try again
      t.socket.SetReadDeadline(time.Now().Add(socket_interval_seconds * time.Second))

      length, err := t.socket.Read(data[received:])
      received += length
      if err == nil || received >= len(data) {
        // Success
        return nil, false
      } else if net_err, ok := err.(net.Error); ok && net_err.Timeout() {
        // Keep trying
        continue
      } else {
        // Pass an error back
        return err, false
      }
    } /* select */
  } /* loop until required amount receive or shutdown */

  return nil, true
}

func (t *TransportTls) setChan(set chan int) {
  select {
  case set <- 1:
  default:
  }
}

func (t *TransportTls) CanSend() chan int {
  return t.can_send
}

func (t *TransportTls) CanRecv() chan int {
  return t.can_recv
}

func (t *TransportTls) Write(p []byte) (int, error) {
  return t.write_buffer.Write(p)
}

func (t *TransportTls) Flush() error {
  t.send_chan <- t.write_buffer.Bytes()
  t.write_buffer.Reset()
  return nil
}

func (t *TransportTls) Read() ([]byte, error) {
  msg := <-t.recv_chan

  // Error? Or data?
  switch msg.(type) {
    case error:
      return nil, msg.(error)
    default:
      return msg.([]byte), nil
  }
}

func (t *TransportTls) Disconnect() {
  // Send shutdown request
  close(t.shutdown)
  t.wait.Wait()
  t.socket.Close()
  t.write_buffer.Reset()
}

func (w *TransportTlsWrap) Read(b []byte) (int, error) {
  return w.tcpsocket.Read(b)
}

func (w *TransportTlsWrap) Write(b []byte) (n int, err error) {
  length := 0

RetrySend:
  for {
    // Timeout after socket_interval_seconds, check for shutdown, and try again
    w.tcpsocket.SetWriteDeadline(time.Now().Add(socket_interval_seconds * time.Second))

    n, err = w.tcpsocket.Write(b[length:])
    length += n
    if err == nil {
      return length, err
    } else if net_err, ok := err.(net.Error); ok && net_err.Timeout() {
      // Check for shutdown, then try again
      select {
      case <-w.transport.shutdown:
        // Shutdown
        return length, err
      default:
        goto RetrySend
      }
    } else {
      return length, err
    }
  } /* loop forever */
}

func (w *TransportTlsWrap) Close() error {
  return w.tcpsocket.Close()
}

func (w *TransportTlsWrap) LocalAddr() net.Addr {
  return w.tcpsocket.LocalAddr()
}

func (w *TransportTlsWrap) RemoteAddr() net.Addr {
  return w.tcpsocket.RemoteAddr()
}

func (w *TransportTlsWrap) SetDeadline(t time.Time) error {
  return w.tcpsocket.SetDeadline(t)
}

func (w *TransportTlsWrap) SetReadDeadline(t time.Time) error {
  return w.tcpsocket.SetReadDeadline(t)
}

func (w *TransportTlsWrap) SetWriteDeadline(t time.Time) error {
  return w.tcpsocket.SetWriteDeadline(t)
}
