package statsd

import (
	"bufio"
	"net"
	"math/rand"
	"fmt"
	//"errors"
	"sync"
	"time"
)

type Client struct {
	// connection buffer
	buf *bufio.ReadWriter
	// underlying connection
	conn *net.Conn
	// prefix for statsd name
	prefix string
	// channels
	data chan string
	quit chan bool

}

// Close closes the connection and cleans up.
func (s *Client) Close() error {
	s.quit <- true
	close(s.quit)
	s.buf.Flush()
	s.buf = nil
	err := (*s.conn).Close()
	return err
}

// Increments a statsd count type.
// stat is a string name for the metric.
// value is the integer value
// rate is the sample rate (0.0 to 1.0)
func (s *Client) Inc(stat string, value int64, rate float32) error {
	dap := fmt.Sprintf("%d|c", value)
	return s.submit(stat, dap, rate)
}

// Decrements a statsd count type.
// stat is a string name for the metric.
// value is the integer value.
// rate is the sample rate (0.0 to 1.0).
func (s *Client) Dec(stat string, value int64, rate float32) error {
	return s.Inc(stat, -value, rate)
}

// Submits/Updates a statsd guage type.
// stat is a string name for the metric.
// value is the integer value.
// rate is the sample rate (0.0 to 1.0).
func (s *Client) Guage(stat string, value int64, rate float32) error {
	dap := fmt.Sprintf("%d|g", value)
	return s.submit(stat, dap, rate)
}

// Submits a statsd timing type.
// stat is a string name for the metric.
// value is the integer value.
// rate is the sample rate (0.0 to 1.0).
func (s *Client) Timing(stat string, delta int64, rate float32) error {
	dap := fmt.Sprintf("%d|ms", delta)
	return s.submit(stat, dap, rate)
}

// Sets/Updates the statsd client prefix
func (s *Client) SetPrefix(prefix string) {
	s.prefix = prefix
}

// submit formats the statsd event data, handles sampling, and prepares it,
// and sends it to the server.
func (s *Client) submit(stat string, value string, rate float32) error {
	if rate < 1 {
		if rand.Float32() < rate {
			value = fmt.Sprintf("%s|@%f", value, rate)
		} else {
			return nil
		}
	}

	if s.prefix != "" {
		stat = fmt.Sprintf("%s.%s", s.prefix, stat)
	}

	s.data <- fmt.Sprintf("%s:%s", stat, value)
	return nil
}

func (s *Client) send(data string) (int, error) {
	n, err := s.buf.Write([]byte(data))
	if err != nil {
		return n, err
	}
	err = s.buf.Flush()
	if err != nil {
		return n, err
	}
	return n, nil
}

// sends the data to the server endpoint over the net.Conn
func (s *Client) StartSender() {
	go func() {
		for {
			select {
			case d := <-s.data:
				_, err := s.send(d)
				// ignore errors. Not sure what to do with an error here...
				// just toss out a log
				if err != nil {
					fmt.Println("error: ", err)
				}
			case <-s.quit:
				return
			}
		}
	}()
}

func newClient(conn *net.Conn, prefix string) *Client {
	buf := bufio.NewReadWriter(bufio.NewReader(*conn), bufio.NewWriter(*conn))
	data := make(chan string, 100)
	quit := make(chan bool)
	client := &Client{
		buf: buf,
		conn: conn,
		prefix: prefix,
		data: data,
		quit: quit}
	client.StartSender()
	return client
}

// Returns a pointer to a new Client.
// addr is a string of the format "hostname:port", and must be parsable by
// net.ResolveUDPAddr.
// prefix is the statsd client prefix. Can be "" if no prefix is desired.
func Dial(addr string, prefix string) (*Client, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}

	client := newClient(&conn, prefix)
	return client, nil
}

// Returns a pointer to a new Client.
// addr is a string of the format "hostname:port", and must be parsable by
// net.ResolveUDPAddr.
// timeout is the connection timeout. Since statsd is UDP, there is no
// connection, but the timeout applies to name resolution (if relevant).
// prefix is the statsd client prefix. Can be "" if no prefix is desired.
func DialTimeout(addr string, timeout time.Duration, prefix string) (*Client, error) {
	conn, err := net.DialTimeout("udp", addr, timeout)
	if err != nil {
		return nil, err
	}

	client := newClient(&conn, prefix)
	return client, nil
}
