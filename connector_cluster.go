package icanal

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/go-zookeeper/zk"
)

type clusterConnector struct {
	destination        string
	opts               []Option
	clusterNodeManager ClusterNodeManager
	simpleConnector    Connector
	zkConn             *zk.Conn
	sequence           string
	retryTimes         int
	retryWait          time.Duration
}

func NewClusterConnector(clusterNodeManager ClusterNodeManager, destination string, opts ...Option) Connector {

	config := getDefaultConfig()

	// 应用所有选项
	for _, opt := range opts {
		opt(config)
	}

	return &clusterConnector{
		destination:        destination,
		opts:               opts,
		clusterNodeManager: clusterNodeManager,
		retryTimes:         config.RetryTimes,
		retryWait:          config.RetryWait,
	}
}

const consumerPath = "/canal-consumer"

func getLockPath(destination string) string {
	return fmt.Sprintf("%s/%s", consumerPath, destination)
}

func (c *clusterConnector) getLock(ctx context.Context) error {

	lockPath := getLockPath(c.destination)

	sequence, err := checkAndCreateEphemeralSequence(c.zkConn, lockPath, c.sequence)
	if err != nil {
		return err
	}
	if sequence != "" {
		c.sequence = sequence
	}

	children, _, err := c.zkConn.Children(lockPath)
	if err != nil {
		return err
	}

	sort.Strings(children)

	if c.sequence != children[0] {
		for i, child := range children {
			if c.sequence == child {
				previousPath := fmt.Sprintf("%s/%s", getLockPath(c.destination), children[i-1])
				if err = waitDelete(c.zkConn, previousPath); err != nil {
					return err
				}
				return c.getLock(ctx)
			}
		}

		// 异常情况，临时节点不存在
		return c.getLock(ctx)
	}

	return nil
}

func (c *clusterConnector) Connect(ctx context.Context) error {

	lockPath := getLockPath(c.destination)
	if err := checkRootPath(c.clusterNodeManager.GetZkConn(), lockPath); err != nil {
		return err
	}

	// 获取锁
	if err := c.getLock(ctx); err != nil {
		return err
	}

	address, err := c.clusterNodeManager.GetNode(ctx)
	if err != nil {
		return err
	}

	c.simpleConnector = NewSimpleConnector(address, c.destination, c.opts...)

	return c.simpleConnector.Connect(ctx)
}

func (c *clusterConnector) restart(ctx context.Context) error {
	if err := c.Disconnect(ctx); err != nil {
		slog.Error("disconnect connector fail", slog.Any("error", err))
	}

	time.Sleep(c.retryWait)

	if err := c.Connect(ctx); err != nil {
		slog.Error("restart connector fail", slog.Any("error", err))
		return err
	}

	return nil
}

func (c *clusterConnector) Disconnect(ctx context.Context) error {
	if c.simpleConnector == nil {
		return nil
	}
	if err := c.simpleConnector.Disconnect(ctx); err != nil {
		return err
	}
	c.simpleConnector = nil

	return nil
}

func (c *clusterConnector) Subscribe(ctx context.Context, filter string) error {
	return c.simpleConnector.Subscribe(ctx, filter)
}

func (c *clusterConnector) Unsubscribe(ctx context.Context) error {
	for times := 0; times < c.retryTimes; times++ {
		if err := c.simpleConnector.Unsubscribe(ctx); err == nil {
			return nil
		}
		_ = c.restart(ctx)
	}
	slog.Error("unsubscribe over retry times")
	return ErrOverRetryTimes
}

func (c *clusterConnector) Get(ctx context.Context, batchSize int32, timeout time.Duration) (*Message, error) {
	for times := 0; times < c.retryTimes; times++ {
		message, err := c.simpleConnector.Get(ctx, batchSize, timeout)
		if err == nil {
			return message, nil
		}
		_ = c.restart(ctx)
	}
	slog.Error("get over retry times")
	return nil, ErrOverRetryTimes
}

func (c *clusterConnector) GetWithoutAck(ctx context.Context, batchSize int32, timeout time.Duration) (*Message, error) {
	for times := 0; times < c.retryTimes; times++ {
		message, err := c.simpleConnector.GetWithoutAck(ctx, batchSize, timeout)
		if err == nil {
			return message, nil
		}
		_ = c.restart(ctx)
	}
	slog.Error("getWithoutAck over retry times")
	return nil, ErrOverRetryTimes
}

func (c *clusterConnector) Ack(ctx context.Context, batchId int64) error {
	for times := 0; times < c.retryTimes; times++ {
		if err := c.simpleConnector.Ack(ctx, batchId); err == nil {
			return nil
		}
		_ = c.restart(ctx)
	}
	slog.Error("ack over retry times")
	return ErrOverRetryTimes
}

func (c *clusterConnector) Rollback(ctx context.Context, batchId int64) error {
	return c.simpleConnector.Rollback(ctx, batchId)
}
