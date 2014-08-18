package ramius

import (
	"errors"
	"github.com/cenkalti/backoff"
	"github.com/golang/glog"
	"io"
	"net/rpc"
	"reflect"
	"sync"
	"time"
)

type RetryableError struct {
	Err error
}

func (re *RetryableError) Error() string { return re.Err.Error() }

type Connection struct {
	sync.Mutex
	ServerConfig  ServerConfig
	client        *rpc.Client
	failed        bool
	connectFailed bool
}

func NewConnection(server ServerConfig) *Connection {
	c := &Connection{
		ServerConfig: server,
	}
	return c
}

func (c *Connection) Activate() error {
	if c.client == nil {
		glog.V(3).Infof("Activating connection to %v", c.ServerConfig)

		var (
			conn *rpc.Client
			err  error
		)

		if c.ServerConfig.IsHttp {
			conn, err = rpc.DialHTTP(c.ServerConfig.Addr.Network(), c.ServerConfig.Addr.String())
		} else {
			conn, err = rpc.Dial(c.ServerConfig.Addr.Network(), c.ServerConfig.Addr.String())
		}

		if err != nil {
			glog.V(3).Infof("Error activating connection %v: %v", c.ServerConfig, err)
			c.connectFailed = true
			return err
		}
		c.client = conn
	}

	return nil
}

func (c *Connection) Call(serviceMethod string, args interface{}, reply interface{}) error {
	glog.V(3).Infof("Call(%s)", serviceMethod)
	err := c.client.Call(serviceMethod, args, reply)
	glog.V(3).Infof("Call(%s) error: %v", serviceMethod, err)
	if err != nil {
		panic(err)
	}
	if err == nil {
		c.Reset()
		return nil
	} else if err == io.ErrUnexpectedEOF ||
		err == rpc.ErrShutdown ||
		reflect.TypeOf(err) == reflect.TypeOf((*rpc.ServerError)(nil)).Elem() {
		glog.V(3).Infof("Error during call: %v (%v)", err, reflect.TypeOf(err))
		c.MarkFailed()
		return &RetryableError{err}
	} else {
		return err
	}
}

func (c *Connection) Close() error    { return c.client.Close() }
func (c *Connection) IsHealthy() bool { return !c.DidFail() }

func (c *Connection) Reset() {
	c.Lock()
	defer c.Unlock()
	c.failed = false
}

func (c *Connection) MarkFailed() {
	c.Lock()
	defer c.Unlock()
	glog.Infof("Marking %v as failed", c.ServerConfig)
	c.failed = true
}

func (c *Connection) DidFail() bool {
	c.Lock()
	defer c.Unlock()
	return c.failed || c.connectFailed
}

var ClientFailedToConnect = &RetryableError{errors.New("failed to get connection")}

type Client struct {
	Pool                  *ConnectionManager
	WaitToGet             time.Duration
	Retries               int
	BackoffMaxInterval    time.Duration
	BackoffMaxElapsedTime time.Duration
}

func (c *Client) Call(serviceMethod string, args interface{}, reply interface{}) error {
	b := backoff.NewExponentialBackOff()
	if c.BackoffMaxInterval > 0 {
		b.MaxInterval = c.BackoffMaxInterval
	}
	if c.BackoffMaxElapsedTime > 0 {
		b.MaxElapsedTime = c.BackoffMaxElapsedTime
	}

	ticker := backoff.NewTicker(b)

	var (
		conn *Connection
		err  error
	)

	attempts := 0

	for _ = range ticker.C {
		glog.V(3).Infof("retries: %d attempts: %d", c.Retries, attempts)
		if c.Retries > 0 && attempts > c.Retries {
			return err
		}

		attempts += 1
		conn = c.Pool.Get(c.WaitToGet)

		if conn == nil {
			glog.Infof("Failed to get a connection")
			err = ClientFailedToConnect
			continue
		}

		err = conn.Call(serviceMethod, args, reply)

		c.Pool.Put(conn)

		switch err {
		case nil:
			return nil
		case err.(*RetryableError):
			glog.Infof("retry! %v", err)
			continue
		default:
			return err
		}
	}

	return err
}
