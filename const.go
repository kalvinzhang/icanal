package icanal

const (
	CanalVersion1 = 1
)

type TimeUint int32

const (
	TimeUintNanoseconds TimeUint = iota
	TimeUnitMicroseconds
	TimeUnitMilliseconds
	TimeUnitSeconds
	TimeUnitMinutes
	TimeUnitHours
	TimeUnitDays
)

const (
	TimeoutDefault = -1 // 立马返回，不阻塞
	TimeoutNever   = 0  // 一直阻塞，直到有数据才返回；主要soTimeout配置
)
