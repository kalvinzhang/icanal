
.DEFAULT_GOAL: help

proto:
	protoc --go_out=. proto/canal_protocol.proto
	protoc --go_out=. proto/entry_protocol.proto

clean:
	rm -rf *.pb.go

help:

.PHONY: proto clean help
