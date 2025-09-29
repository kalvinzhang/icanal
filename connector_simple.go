package icanal

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/kalvinzhang/icanal/protocol/canal"
)

type simpleConnector struct {
	conn           net.Conn
	mutex          sync.Mutex
	connected      bool
	running        bool
	config         *ConnectorConfig
	address        string
	clientIdentity ClientIdentity
}

// NewSimpleConnector 新建简单连接器
func NewSimpleConnector(address string, destination string, opts ...Option) Connector {

	config := getDefaultConfig()

	// 应用所有选项
	for _, opt := range opts {
		opt(config)
	}

	return &simpleConnector{
		config:  config,
		address: address,
		clientIdentity: ClientIdentity{
			Destination: destination,
			ClientId:    ClientIdDefault,
			Filter:      "",
		},
	}
}

// Connect 连接到server
func (c *simpleConnector) Connect(ctx context.Context) error {
	if c.connected {
		return nil
	}

	c.waitClientRunning()
	if !c.running {
		return nil
	}

	if err := c.doConnect(ctx); err != nil {
		return err
	}

	if c.config.Filter != "" {
		if err := c.Subscribe(ctx, c.config.Filter); err != nil {
			return err
		}
	}

	if c.config.RollbackOnConnect {
		if err := c.Rollback(ctx, 0); err != nil {
			return err
		}
	}

	return nil
}

func (c *simpleConnector) doConnect(ctx context.Context) error {
	conn, err := net.Dial("tcp", c.address)
	if err != nil {
		return err
	}
	c.conn = conn

	handshake, err := c.handshake(ctx)
	if err != nil {
		return err
	}

	ca := &canal.ClientAuth{
		Username:               c.config.Username,
		Password:               []byte(hex.EncodeToString(scramble411([]byte(c.config.Password), handshake.GetSeeds()))),
		NetReadTimeoutPresent:  &canal.ClientAuth_NetReadTimeout{NetReadTimeout: int32(c.config.IdleTimeout.Milliseconds())},
		NetWriteTimeoutPresent: &canal.ClientAuth_NetWriteTimeout{NetWriteTimeout: int32(c.config.IdleTimeout.Milliseconds())},
	}
	data := marshalPacketIgnoreError(canal.PacketType_CLIENTAUTHENTICATION, ca)

	if err = c.writeWithHeader(data); err != nil {
		return err
	}

	packet, err := c.readNextPacket(ctx)
	if err != nil {
		return err
	}

	if packet.GetType() != canal.PacketType_ACK {
		slog.ErrorContext(ctx, "expect packet type but found other type")
		return ErrExpectedPacketType
	}

	ack := &canal.Ack{}
	if err = proto.Unmarshal(packet.GetBody(), ack); err != nil {
		return errors.Join(ErrUnmarshal, err)
	}
	if ack.GetErrorCode() > 0 {
		return errors.Join(ErrAuth, NewCanalError(ack.GetErrorCode(), ack.GetErrorMessage()))
	}

	c.connected = true

	slog.InfoContext(ctx, "connected",
		slog.String("destination", c.clientIdentity.Destination),
	)

	return nil
}

func (c *simpleConnector) handshake(ctx context.Context) (*canal.Handshake, error) {
	packet, err := c.readNextPacket(ctx)
	if err != nil {
		return nil, err
	}
	if packet.GetVersion() != CanalVersion1 {
		return nil, ErrUnsupportedVersion
	}

	if packet.GetType() != canal.PacketType_HANDSHAKE {
		return nil, ErrHandshake
	}

	handshake := &canal.Handshake{}
	if err = proto.Unmarshal(packet.GetBody(), handshake); err != nil {
		return nil, errors.Join(ErrUnmarshal, err)
	}
	return handshake, nil
}

// Subscribe 订阅
func (c *simpleConnector) Subscribe(ctx context.Context, filter string) error {
	c.waitClientRunning()
	if !c.running {
		return nil
	}

	data := marshalPacketIgnoreError(canal.PacketType_SUBSCRIPTION, &canal.Sub{
		Destination: c.clientIdentity.Destination,
		ClientId:    strconv.Itoa(c.clientIdentity.ClientId),
		Filter:      filter,
	})

	if err := c.writeWithHeader(data); err != nil {
		return err
	}

	packet, err := c.readNextPacket(ctx)
	if err != nil {
		return err
	}

	ack := &canal.Ack{}
	if err = proto.Unmarshal(packet.GetBody(), ack); err != nil {
		return errors.Join(ErrUnmarshal, err)
	}

	if ack.GetErrorCode() > 0 {
		return errors.Join(ErrSubscribe, NewCanalError(ack.GetErrorCode(), ack.GetErrorMessage()))
	}

	c.clientIdentity.Filter = filter

	slog.InfoContext(ctx, "subscribe",
		slog.String("destination", c.clientIdentity.Destination),
		slog.String("filter", filter),
	)

	return nil
}

// GetWithoutAck 获取数据不确认
func (c *simpleConnector) GetWithoutAck(ctx context.Context, batchSize int32, timeout time.Duration) (*Message, error) {
	c.waitClientRunning()
	if !c.running {
		return nil, nil
	}

	if batchSize <= 0 {
		batchSize = BatchSizeDefault
	}

	var innerTimeout int64 = TimeoutDefault

	if timeout >= 0 {
		innerTimeout = timeout.Milliseconds()
	}

	get := &canal.Get{
		Destination:    c.clientIdentity.Destination,
		ClientId:       strconv.Itoa(c.clientIdentity.ClientId),
		FetchSize:      batchSize,
		TimeoutPresent: &canal.Get_Timeout{Timeout: innerTimeout},
		UnitPresent:    &canal.Get_Unit{Unit: int32(TimeUnitMilliseconds)},
		AutoAckPresent: &canal.Get_AutoAck{AutoAck: false},
	}
	data := marshalPacketIgnoreError(canal.PacketType_GET, get)

	if err := c.writeWithHeader(data); err != nil {
		return nil, err
	}
	message, err := c.receiveMessage(ctx)
	if err != nil {
		return nil, err
	}
	return message, nil
}

// receiveMessage 接受消息
func (c *simpleConnector) receiveMessage(ctx context.Context) (*Message, error) {
	packet, err := c.readNextPacket(ctx)
	if err != nil {
		return nil, err
	}

	return decodeMessages(packet, c.config.LazyParseEntry)
}

// waitClientRunning 等待客户端执行
func (c *simpleConnector) waitClientRunning() {
	c.running = true
}

// Rollback 回滚
func (c *simpleConnector) Rollback(ctx context.Context, batchId int64) error {
	c.waitClientRunning()

	data := marshalPacketIgnoreError(canal.PacketType_CLIENTROLLBACK, &canal.ClientRollback{
		Destination: c.clientIdentity.Destination,
		ClientId:    strconv.Itoa(c.clientIdentity.ClientId),
		BatchId:     batchId,
	})

	return c.writeWithHeader(data)
}

const typeLength = 4

// readNetPacket 读取下一个网络包
func (c *simpleConnector) readNetPacket(_ context.Context) ([]byte, error) {
	c.mutex.Lock()
	defer func() {
		c.mutex.Unlock()
	}()

	// 设置超时
	if err := c.conn.SetReadDeadline(time.Now().Add(c.config.SoTimeout)); err != nil {
		return nil, err
	}

	reader := bufio.NewReader(c.conn)

	lengthBytes := make([]byte, typeLength)

	if _, err := io.ReadFull(reader, lengthBytes); err != nil {
		return nil, err
	}

	dataLen := binary.BigEndian.Uint32(lengthBytes)
	data := make([]byte, dataLen)

	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, err
	}

	// 清空超时
	if err := c.conn.SetWriteDeadline(time.Time{}); err != nil {
		return data, err
	}

	return data, nil
}

func (c *simpleConnector) readNextPacket(ctx context.Context) (*canal.Packet, error) {
	np, err := c.readNetPacket(ctx)
	if err != nil {
		return nil, err
	}

	pk := &canal.Packet{}
	if err = proto.Unmarshal(np, pk); err != nil {
		return nil, errors.Join(ErrUnmarshal, err)
	}

	return pk, nil
}

// writeWithHeader 写网络头
func (c *simpleConnector) writeWithHeader(data []byte) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	length := len(data)
	header := generateWriteHeader(length)

	// 设置超时
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.config.SoTimeout)); err != nil {
		return err
	}

	if _, err := c.conn.Write(header); err != nil {
		return err
	}
	if _, err := c.conn.Write(data); err != nil {
		return err
	}

	// 清空超时
	if err := c.conn.SetWriteDeadline(time.Time{}); err != nil {
		return err
	}

	return nil
}

// Disconnect 断开连接
func (c *simpleConnector) Disconnect(ctx context.Context) error {
	if c.config.RollbackOnDisconnect && c.connected {
		if err := c.Rollback(ctx, 0); err != nil {
			return err
		}
	}

	c.connected = false

	if err := c.conn.Close(); err != nil {
		slog.ErrorContext(ctx, "failed to disconnect",
			slog.Any("error", err))
		return err
	}

	return nil
}

// Unsubscribe 取消订阅
func (c *simpleConnector) Unsubscribe(ctx context.Context) error {
	c.waitClientRunning()
	if !c.running {
		return nil
	}

	data := marshalPacketIgnoreError(canal.PacketType_UNSUBSCRIPTION, &canal.Unsub{
		Destination: c.clientIdentity.Destination,
		ClientId:    strconv.Itoa(c.clientIdentity.ClientId),
	})

	if err := c.writeWithHeader(data); err != nil {
		return err
	}

	packet, err := c.readNextPacket(ctx)
	if err != nil {
		return err
	}

	ack := &canal.Ack{}
	if err = proto.Unmarshal(packet.GetBody(), ack); err != nil {
		return errors.Join(ErrUnmarshal, err)
	}

	if ack.GetErrorCode() > 0 {
		return errors.Join(ErrUnsubscribe, NewCanalError(ack.GetErrorCode(), ack.GetErrorMessage()))
	}

	return nil
}

// Get 获取消息
func (c *simpleConnector) Get(ctx context.Context, batchSize int32, timeout time.Duration) (*Message, error) {
	message, err := c.GetWithoutAck(ctx, batchSize, timeout)
	if err != nil {
		return nil, err
	}
	if message == nil {
		return nil, errors.New("message is nil")
	}

	if err = c.Ack(ctx, message.Id); err != nil {
		return nil, err
	}

	return message, nil
}

// Ack 确认
func (c *simpleConnector) Ack(ctx context.Context, batchId int64) error {
	c.waitClientRunning()
	if !c.running {
		return nil
	}

	data := marshalPacketIgnoreError(canal.PacketType_CLIENTACK, &canal.ClientAck{
		Destination: c.clientIdentity.Destination,
		ClientId:    strconv.Itoa(c.clientIdentity.ClientId),
		BatchId:     batchId,
	})

	if err := c.writeWithHeader(data); err != nil {
		return err
	}

	return nil
}
