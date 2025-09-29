package icanal

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/go-zookeeper/zk"
)

// ClusterManager 集群经理；处理集群节点选取和分布式锁
type ClusterManager interface {
	Init(ctx context.Context) error
	GetNode(ctx context.Context) (string, error)
	GetLock(ctx context.Context) error // 获取锁
}

type clusterManager struct {
	destination    string
	zkServer       []string
	sessionTimeout time.Duration
	zkConn         *zk.Conn
	clusterAddress []string
	init           bool
	lockSequence   string
}

// NewClusterNodeManager 新建集群节点经理
func NewClusterNodeManager(destination string, zkServer []string, sessionTimeout time.Duration) ClusterManager {
	return &clusterManager{
		destination:    destination,
		zkServer:       zkServer,
		sessionTimeout: sessionTimeout,
	}
}

func (m *clusterManager) Init(ctx context.Context) error {
	if m.init {
		return nil
	}
	if err := m.connectZookeeper(ctx); err != nil {
		return err
	}

	if err := m.getClustersAndInit(ctx); err != nil {
		return err
	}

	// 坚持锁路径
	if err := checkRootPath(m.zkConn, getLockPath(m.destination)); err != nil {
		return err
	}

	m.init = true

	return nil
}

func (m *clusterManager) connectZookeeper(ctx context.Context) error {
	zkConn, _, err := zk.Connect(m.zkServer, m.sessionTimeout)
	if err != nil {
		slog.ErrorContext(ctx, "connect zookeeper error",
			slog.Any("error", err),
			slog.Any("zkServer", m.zkServer),
		)
		return err
	}

	m.zkConn = zkConn

	return nil
}

func (m *clusterManager) getClustersAndInit(ctx context.Context) error {
	cluster, _, _, err := m.zkConn.ChildrenW(getDestinationCluster(m.destination))
	if err != nil {
		slog.ErrorContext(ctx, "zookeeper get children error",
			slog.Any("error", err))
		return err
	}

	m.initClusters(cluster)

	return nil
}

func (m *clusterManager) initClusters(addressList []string) {
	rand.Shuffle(len(addressList), func(a, b int) {
		addressList[a], addressList[b] = addressList[b], addressList[a]
	})
	m.clusterAddress = addressList
}

func (m *clusterManager) GetNode(ctx context.Context) (string, error) {
	runningData, err := m.getRunningServerData(ctx)
	if err != nil {
		return "", err
	}
	return runningData.Address, nil
}

// serverRunningData 服务运行数据
type serverRunningData struct {
	Cid     int64  `json:"cid"`
	Address string `json:"address"`
	Active  bool   `json:"active"`
}

func (m *clusterManager) getRunningServerData(ctx context.Context) (*serverRunningData, error) {

	body, _, err := m.zkConn.Get(getDestinationServerRunning(m.destination))
	if err != nil {
		slog.WarnContext(ctx, "get server running data error", slog.Any("error", err))
		return nil, err
	}

	serverInfo := serverRunningData{}
	if err = json.Unmarshal(body, &serverInfo); err != nil {
		slog.ErrorContext(ctx, "unmarshal server running data error", slog.Any("error", err))
		return nil, err
	}

	return &serverInfo, nil
}

func getDestinationServerRunning(destination string) string {
	return fmt.Sprintf("/otter/canal/destinations/%s/running", destination)
}

func getDestinationCluster(destination string) string {
	return fmt.Sprintf("/otter/canal/destinations/%s/cluster", destination)
}

// GetLock 获取锁
func (m *clusterManager) GetLock(ctx context.Context) error {

	lockPath := getLockPath(m.destination)

	sequence, err := checkAndCreateEphemeralSequence(m.zkConn, lockPath, m.lockSequence)
	if err != nil {
		return err
	}
	if sequence != "" {
		m.lockSequence = sequence
	}

	children, _, err := m.zkConn.Children(lockPath)
	if err != nil {
		return err
	}

	sort.Strings(children)

	if m.lockSequence != children[0] {
		for i, child := range children {
			if m.lockSequence == child {
				previousPath := fmt.Sprintf("%s/%s", getLockPath(m.destination), children[i-1])
				if err = waitDelete(m.zkConn, previousPath); err != nil {
					return err
				}
				return m.GetLock(ctx)
			}
		}

		// 异常情况，临时节点不存在
		return m.GetLock(ctx)
	}

	return nil
}
