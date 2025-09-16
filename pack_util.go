package icanal

import (
	"google.golang.org/protobuf/proto"

	"github.com/kalvinzhang/icanal/protocol"
)

// marshalPacket 生成pack数据
func marshalPacket[T proto.Message](packetType protocol.PacketType, payload T) ([]byte, error) {
	body, err := proto.Marshal(payload)
	if err != nil {
		return nil, err
	}

	packet := &protocol.Packet{
		Type: packetType,
		Body: body,
	}

	return proto.Marshal(packet)
}

// marshalPacketIgnoreError 生成pack数据，忽略错误
func marshalPacketIgnoreError[T proto.Message](packetType protocol.PacketType, payload T) []byte {
	data, _ := marshalPacket(packetType, payload)
	return data
}
