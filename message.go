// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package icanal

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

type Message struct {
	Id         int64
	Entries    []*Entry
	Raw        bool
	RawEntries any
}

func decodeMessages(packet *Packet, lazyParseEntry bool) (*Message, error) {

	switch packet.GetType() {
	case PacketType_MESSAGES:
		if !(packet.GetCompression() == Compression_NONE) &&
			!(packet.GetCompression() == Compression_COMPRESSIONCOMPATIBLEPROTO2) {
			return nil, ErrCompressionNotSupport
		}

		messages := &Messages{}
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
	case PacketType_ACK:
		ack := &Ack{}
		if err := proto.Unmarshal(packet.GetBody(), ack); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("something goes wrong with reason: %s", ack.GetErrorMessage())
	default:
		return nil, fmt.Errorf("unexpected packet type: %s", packet.GetType())
	}
}
