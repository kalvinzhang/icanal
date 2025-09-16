package icanal

import "errors"

var (
	ErrUnsupportedVersion    = errors.New("unsupported version at this client")
	ErrHandshake             = errors.New("expect handshake but found other type")
	ErrUnmarshal             = errors.New("unmarshal error")
	ErrCompressionNotSupport = errors.New("compression is not supported in this connector")
	ErrNetwork               = errors.New("network error")
	ErrOverRetryTimes        = errors.New("over retry times")
)
