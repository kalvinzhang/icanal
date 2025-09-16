package icanal

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/go-zookeeper/zk"
)

// ClusterNodeManager 集群node经理
type ClusterNodeManager interface {
	Init(ctx context.Context) error
	GetZkConn() *zk.Conn
	GetNode(ctx context.Context) (string, error)
}

type clusterNodeManager struct {
	destination    string
	zkServer       []string
	timeout        time.Duration
	zkConn         *zk.Conn
	clusterAddress []string
	init           bool
}

func NewClusterManager(destination string, zkServer []string, timeout time.Duration) ClusterNodeManager {
	return &clusterNodeManager{
		destination: destination,
		zkServer:    zkServer,
		timeout:     timeout,
	}
}

func (m *clusterNodeManager) GetZkConn() *zk.Conn {
	return m.zkConn
}

func (m *clusterNodeManager) Init(ctx context.Context) error {
	if m.init {
		return nil
	}
	if err := m.connectZookeeper(ctx); err != nil {
		return err
	}

	if err := m.getClustersAndInit(ctx); err != nil {
		return err
	}

	m.init = true

	return nil
}

func (m *clusterNodeManager) connectZookeeper(ctx context.Context) error {
	zkClient, _, err := zk.Connect(m.zkServer, m.timeout)
	if err != nil {
		slog.ErrorContext(ctx, "connect zookeeper error",
			slog.Any("error", err),
			slog.Any("zkServer", m.zkServer),
		)
		return err
	}

	m.zkConn = zkClient

	return nil
}

func (m *clusterNodeManager) getClustersAndInit(ctx context.Context) error {
	cluster, _, _, err := m.zkConn.ChildrenW(getDestinationCluster(m.destination))
	if err != nil {
		slog.ErrorContext(ctx, "zookeeper get children error",
			slog.Any("error", err))
		return err
	}

	m.initClusters(cluster)

	return nil
}

func (m *clusterNodeManager) initClusters(addressList []string) {
	rand.Shuffle(len(addressList), func(a, b int) {
		addressList[a], addressList[b] = addressList[b], addressList[a]
	})
	m.clusterAddress = addressList
}

func (m *clusterNodeManager) GetNode(ctx context.Context) (string, error) {
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

func (m *clusterNodeManager) getRunningServerData(ctx context.Context) (*serverRunningData, error) {

	body, _, err := m.zkConn.Get(getDestinationServerRunning(m.destination))
	if err != nil {
		slog.ErrorContext(ctx, "get server running data error", slog.Any("error", err))
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
