package icanal

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/kalvinzhang/icanal/protocol/canal"
)

type Message struct {
	Id         int64
	Entries    []*Entry
	Raw        bool
	RawEntries any
}

func decodeMessages(packet *canal.Packet, lazyParseEntry bool) (*Message, error) {

	switch packet.GetType() {
	case canal.PacketType_MESSAGES:
		if !(packet.GetCompression() == canal.Compression_NONE) &&
			!(packet.GetCompression() == canal.Compression_COMPRESSIONCOMPATIBLEPROTO2) {
			return nil, ErrCompressionNotSupport
		}

		messages := &canal.Messages{}
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
	case canal.PacketType_ACK:
		ack := &canal.Ack{}
		if err := proto.Unmarshal(packet.GetBody(), ack); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("something goes wrong with reason: %s", ack.GetErrorMessage())
	default:
		return nil, fmt.Errorf("unexpected packet type: %s", packet.GetType())
	}
}
