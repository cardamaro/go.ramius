package ramius

import (
	"errors"
	"fmt"
	"github.com/cardamaro/stats"
	"github.com/cenkalti/backoff"
	"github.com/golang/glog"
	"io"
	"net/rpc"
	"reflect"
	"sync"
	"time"
)

var (
	callTimings               = stats.NewTimings("RamiusRpcConnCallLatency")
	countConnRetryableErrors  = stats.NewCounters("RamiusRpcConnCallRetryableErr")
	countConnErrors           = stats.NewCounters("RamiusRpcConnCallErr")
	countActivations          = stats.NewInt("RamiusRpcConnActivate")
	countActivationsDial      = stats.NewInt("RamiusRpcConnActivateDial")
	countActivationsByAddr    = stats.NewCounters("RamiusRpcConnActivationsByAddr")
	countActivationsByAddrErr = stats.NewCounters("RamiusRpcConnActivationsByAddrErr")
	countFailedToGetConn      = stats.NewInt("RamiusRpcFailedToGetConn")
	countConnErrorsByType     = stats.NewCounters("RamiusRpcConnErrByType")
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
	countActivations.Add(1)

	if c.client == nil {
		glog.V(3).Infof("Activating connection to %v", c.ServerConfig)
		countActivationsDial.Add(1)

		var (
			conn *rpc.Client
			err  error
		)

		countActivationsByAddr.Add(c.ServerConfig.Addr.String(), 1)

		if c.ServerConfig.IsHttp {
			conn, err = rpc.DialHTTP(c.ServerConfig.Addr.Network(), c.ServerConfig.Addr.String())
		} else {
			conn, err = rpc.Dial(c.ServerConfig.Addr.Network(), c.ServerConfig.Addr.String())
		}

		if err != nil {
			countActivationsByAddrErr.Add(c.ServerConfig.Addr.String(), 1)
			countConnErrorsByType.Add(fmt.Sprintf("%T", err), 1)
			glog.V(3).Infof("Error activating connection %v: %v", c.ServerConfig, err)
			c.connectFailed = true
			return err
		}
		c.client = conn
	}

	return nil
}

func (c *Connection) Call(serviceMethod string, args interface{}, reply interface{}) error {
	glog.V(4).Infof("Call(%s)", serviceMethod)
	defer callTimings.Record(serviceMethod, time.Now())
	err := c.client.Call(serviceMethod, args, reply)
	if err == nil {
		c.Reset()
		return nil
	} else if err == io.ErrUnexpectedEOF ||
		err == rpc.ErrShutdown ||
		reflect.TypeOf(err) == reflect.TypeOf((*rpc.ServerError)(nil)).Elem() {
		countConnRetryableErrors.Add(serviceMethod, 1)
		glog.V(3).Infof("Error during Call(%s): %v (%v)", serviceMethod, err, reflect.TypeOf(err))
		c.MarkFailed()
		return &RetryableError{err}
	} else {
		countConnErrors.Add(serviceMethod, 1)
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
	glog.V(4).Infof("Backoff config: %+v", b)

	ticker := backoff.NewTicker(b)

	var (
		conn *Connection
		err  error
	)

	for _ = range ticker.C {
		conn = c.Pool.Get(c.WaitToGet)

		if conn == nil {
			glog.V(3).Infof("Failed to get a connection")
			countFailedToGetConn.Add(1)
			err = ClientFailedToConnect
			continue
		}

		err = conn.Call(serviceMethod, args, reply)

		if err != nil {
			countConnErrorsByType.Add(fmt.Sprintf("%T", err), 1)
		}

		c.Pool.Put(conn)

		switch err {
		case nil:
			return nil
		case err.(*RetryableError):
			glog.V(3).Infof("retry! %v", err)
			continue
		default:
			return err
		}
	}

	return err
}
