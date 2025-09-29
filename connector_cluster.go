package icanal

import (
	"context"
	"log/slog"
	"time"
)

type clusterConnector struct {
	destination     string
	opts            []Option
	clusterManager  ClusterManager
	simpleConnector Connector
	retryTimes      int
	retryWait       time.Duration
}

// NewClusterConnector 新建集群连接器
func NewClusterConnector(destination string, zkServer []string, zkSessionTimeout time.Duration, opts ...Option) Connector {

	config := getDefaultConfig()

	// 应用所有选项
	for _, opt := range opts {
		opt(config)
	}

	return &clusterConnector{
		destination:    destination,
		opts:           opts,
		clusterManager: NewClusterNodeManager(destination, zkServer, zkSessionTimeout),
		retryTimes:     config.RetryTimes,
		retryWait:      config.RetryInterval,
	}
}

func (c *clusterConnector) Connect(ctx context.Context) error {
	if err := c.clusterManager.Init(ctx); err != nil {
		return err
	}

	// 获取锁
	if err := c.clusterManager.GetLock(ctx); err != nil {
		return err
	}

	for times := 0; times < c.retryTimes; times++ {
		if err := c.getNodeAndConnect(ctx); err == nil { // 成功直接返回
			return nil
		}

		slog.WarnContext(ctx, "failed to connect to canal server after retry",
			slog.Int("retry_times", times))

		if times < c.retryTimes-1 {
			time.Sleep(c.retryWait)
		}
	}

	slog.ErrorContext(ctx, "failed to connect over retry times")
	return ErrOverRetryTimes
}

func (c *clusterConnector) getNodeAndConnect(ctx context.Context) error {
	address, err := c.clusterManager.GetNode(ctx)
	if err != nil {
		return err
	}

	c.simpleConnector = NewSimpleConnector(address, c.destination, c.opts...)

	if err = c.simpleConnector.Connect(ctx); err != nil {
		return err
	}

	return nil
}

func (c *clusterConnector) restart(ctx context.Context) error {
	if err := c.Disconnect(ctx); err != nil {
		slog.WarnContext(ctx, "failed to disconnect", slog.Any("error", err))
	}

	time.Sleep(c.retryWait)

	if err := c.Connect(ctx); err != nil {
		slog.ErrorContext(ctx, "restart connector fail", slog.Any("error", err))
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
	for times := 0; times < c.retryTimes; times++ {
		if err := c.simpleConnector.Subscribe(ctx, filter); err == nil {
			return nil
		}
		if err := c.restart(ctx); err != nil {
			return err
		}
	}
	slog.ErrorContext(ctx, "failed to subscribe over retry times")
	return ErrOverRetryTimes
}

func (c *clusterConnector) Unsubscribe(ctx context.Context) error {
	for times := 0; times < c.retryTimes; times++ {
		if err := c.simpleConnector.Unsubscribe(ctx); err == nil {
			return nil
		}
		if err := c.restart(ctx); err != nil {
			return err
		}
	}
	slog.ErrorContext(ctx, "failed to unsubscribe over retry times")
	return ErrOverRetryTimes
}

func (c *clusterConnector) Get(ctx context.Context, batchSize int32, timeout time.Duration) (*Message, error) {
	for times := 0; times < c.retryTimes; times++ {
		message, err := c.simpleConnector.Get(ctx, batchSize, timeout)
		if err == nil {
			return message, nil
		}
		if err = c.restart(ctx); err != nil {
			return nil, err
		}
	}
	slog.ErrorContext(ctx, "failed to get over retry times")
	return nil, ErrOverRetryTimes
}

func (c *clusterConnector) GetWithoutAck(ctx context.Context, batchSize int32, timeout time.Duration) (*Message, error) {
	for times := 0; times < c.retryTimes; times++ {
		message, err := c.simpleConnector.GetWithoutAck(ctx, batchSize, timeout)
		if err == nil {
			return message, nil
		}
		if err = c.restart(ctx); err != nil {
			return nil, err
		}
	}
	slog.ErrorContext(ctx, "failed to get without ack over retry times")
	return nil, ErrOverRetryTimes
}

func (c *clusterConnector) Ack(ctx context.Context, batchId int64) error {
	for times := 0; times < c.retryTimes; times++ {
		if err := c.simpleConnector.Ack(ctx, batchId); err == nil {
			return nil
		}
		if err := c.restart(ctx); err != nil {
			return err
		}
	}
	slog.ErrorContext(ctx, "failed to ack over retry times")
	return ErrOverRetryTimes
}

func (c *clusterConnector) Rollback(ctx context.Context, batchId int64) error {
	for times := 0; times < c.retryTimes; times++ {
		if err := c.simpleConnector.Rollback(ctx, batchId); err == nil {
			return nil
		}
		if err := c.restart(ctx); err != nil {
			return err
		}
	}
	slog.ErrorContext(ctx, "failed to rollback over retry times")
	return ErrOverRetryTimes
}
