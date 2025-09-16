package icanal

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/kalvinzhang/icanal/protocol"
)

type Message struct {
	Id         int64
	Entries    []*Entry
	Raw        bool
	RawEntries any
}

func decodeMessages(packet *protocol.Packet, lazyParseEntry bool) (*Message, error) {

	switch packet.GetType() {
	case protocol.PacketType_MESSAGES:
		if !(packet.GetCompression() == protocol.Compression_NONE) &&
			!(packet.GetCompression() == protocol.Compression_COMPRESSIONCOMPATIBLEPROTO2) {
			return nil, ErrCompressionNotSupport
		}

		messages := &protocol.Messages{}
		if err := proto.Unmarshal(packet.GetBody(), messages); err != nil {
			return nil, err
		}

		message := &Message{
			Id: messages.GetBatchId(),
		}

		if lazyParseEntry {
			message.RawEntries = messages.GetMessages()
			message.Raw = true
		} else {
			var entries []*Entry

			for _, value := range messages.GetMessages() {
				entry := &Entry{}
				if err := proto.Unmarshal(value, entry); err != nil {
					return nil, err
				}
				entries = append(entries, entry)
			}

			message.Entries = entries
		}

		return message, nil
	case protocol.PacketType_ACK:
		ack := &protocol.Ack{}
		if err := proto.Unmarshal(packet.GetBody(), ack); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("something goes wrong with reason: %s", ack.GetErrorMessage())
	default:
		return nil, fmt.Errorf("unexpected packet type: %s", packet.GetType())
	}
}
