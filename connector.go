package icanal

import (
	"context"
	"time"
)

// Connector 连接器
type Connector interface {
	Connect(ctx context.Context) error
	Disconnect(ctx context.Context) error
	Subscribe(ctx context.Context, filter string) error
	Unsubscribe(ctx context.Context) error
	Get(ctx context.Context, batchSize int32, timeout time.Duration) (*Message, error)
	GetWithoutAck(ctx context.Context, batchSize int32, timeout time.Duration) (*Message, error)
	Ack(ctx context.Context, batchId int64) error
	Rollback(ctx context.Context, batchId int64) error
}

// ClientIdentity 客户端标识
type ClientIdentity struct {
	Destination string
	ClientId    int
	Filter      string
}
