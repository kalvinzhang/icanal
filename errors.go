package icanal

import "errors"

var (
	ErrUnsupportedVersion    = errors.New("unsupported version at this client")
	ErrHandshake             = errors.New("expect handshake but found other type")
	ErrExpectedPacketType    = errors.New("expect packet type but found other type")
	ErrAuth                  = errors.New("auth error")
	ErrUnmarshal             = errors.New("unmarshal error")
	ErrCompressionNotSupport = errors.New("compression is not supported in this connector")
	ErrNetwork               = errors.New("network error")
	ErrOverRetryTimes        = errors.New("over retry times")
	ErrSubscribe             = errors.New("subscribe error")
	ErrUnsubscribe           = errors.New("unsubscribe error")
)

type CanalError struct {
	Code int32
	Msg  string
}

func (e *CanalError) Error() string {
	return e.Msg
}

func NewCanalError(code int32, msg string) error {
	return &CanalError{
		Code: code,
		Msg:  msg,
	}
}
