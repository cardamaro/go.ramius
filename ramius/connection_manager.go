package ramius

import (
	"fmt"
	"github.com/golang/glog"
	"net"
	"sync"
	"time"
)

type ServerConfig struct {
	Addr   net.Addr
	IsHttp bool
}

func (s ServerConfig) String() string {
	return fmt.Sprintf("ServerConfig{Addr: %v, IsHttp: %t}", s.Addr, s.IsHttp)
}

type ServerSet struct {
	sync.Mutex
	servers          []ServerConfig
	quarantinePeriod time.Duration
	quarantined      map[ServerConfig]time.Time
}

func NewServerSet(servers []ServerConfig, quarantinePeriod time.Duration) *ServerSet {
	return &ServerSet{
		servers:          servers,
		quarantinePeriod: quarantinePeriod,
		quarantined:      make(map[ServerConfig]time.Time),
	}
}

func (s *ServerSet) Quarantine(server ServerConfig) {
	s.Lock()
	defer s.Unlock()
	glog.V(3).Infof("Quarantining %v for %v", server, s.quarantinePeriod)
	s.quarantined[server] = time.Now()
}

func (s *ServerSet) All() []ServerConfig {
	s.Lock()
	defer s.Unlock()

	var all []ServerConfig

	for _, server := range s.servers {
		if ts, ok := s.quarantined[server]; ok {
			if time.Since(ts) < s.quarantinePeriod {
				glog.V(3).Infof("%v quarantined until %v", server, s.quarantinePeriod-time.Since(ts))
				continue
			} else {
				glog.V(3).Infof("%v no longer quarantined", server)
				delete(s.quarantined, server)
			}
		}

		all = append(all, server)
	}

	return all
}

type ConnectionManager struct {
	sync.Mutex
	Active  int
	size    int
	servers *ServerSet
	pooled  map[ServerConfig]bool
	conns   chan *Connection
}

func NewConnectionManager(servers *ServerSet, size int) *ConnectionManager {
	return &ConnectionManager{
		size:    size,
		servers: servers,
		pooled:  make(map[ServerConfig]bool),
		conns:   make(chan *Connection, size),
	}
}

func (m *ConnectionManager) Init() {
	m.tryToFillPool()
}

func (m *ConnectionManager) tryToFillPool() {
	c := len(m.conns) + m.Active
	need := m.size - c
	glog.V(3).Infof("size: %d total conns: %d (%d + %d) need: %d", m.size, c, len(m.conns), m.Active, need)

	if need == 0 {
		return
	}

	for _, server := range m.servers.All() {
		if need == 0 {
			break
		}
		if _, ok := m.pooled[server]; ok {
			continue
		}
		glog.V(3).Infof("Adding %v to pool", server)
		m.conns <- NewConnection(server)
		m.pooled[server] = true
		need -= 1
	}
	glog.V(3).Infof("got: %d", (m.size-c)-need)
}

func (m *ConnectionManager) removeConn(conn *Connection) {
	glog.V(3).Infof("Removing %v from pool", conn.ServerConfig)
	delete(m.pooled, conn.ServerConfig)

	if !conn.IsHealthy() {
		m.servers.Quarantine(conn.ServerConfig)
	}
}

func (m *ConnectionManager) Get(timeout time.Duration) *Connection {
	for {
		select {
		case conn := <-m.conns:
			if err := conn.Activate(); err != nil {
				m.Lock()
				m.removeConn(conn)
				m.Unlock()
				continue
			}

			m.Lock()
			m.Active += 1
			m.Unlock()

			glog.V(3).Infof("Get %v active: %d", conn.ServerConfig, m.Active)

			return conn
		case <-time.After(timeout):
			glog.Infof("Timed out waiting to get a connection (%s)", timeout)
			m.tryToFillPool()
			return nil
		}
	}
}

func (m *ConnectionManager) Put(conn *Connection) {
	if conn == nil {
		return
	}

	m.Lock()
	defer m.Unlock()

	m.Active -= 1
	glog.V(3).Infof("Put %v active: %d", conn.ServerConfig, m.Active)

	if conn.IsHealthy() {
		m.conns <- conn
	} else {
		m.removeConn(conn)
	}

	m.tryToFillPool()

	/*
	  var toBeDestroyed *Connection

	  if conn.IsHealthy() {
	    conn.Passivate()
	    m.conns <- conn
	    glog.V(3).Infof('pool size: %d, maxIdle: %d', len(m.conns), m.maxIdle)
	    if len(m.conns) > m.maxIdle {
	      toBeDestroyed = <-m.conns
	    }
	    glog.Infof('Returned connection %v (active: %d)', conn, m.Active)
	  } else {
	    toBeDestroyed = conn
	  }

	  if toBeDestroyed != nil {
	    glog.Infof("Destroying %v", toBeDestroyed)
	    if !toBeDestroyed.IsHealthy() {
	      m.servers.Quarantine(toBeDestroyed)
	    }
	    toBeDestroyed.Destroy()
	  }
	*/
}
