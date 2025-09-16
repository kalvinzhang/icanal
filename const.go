package icanal

const (
	CanalVersion = 1
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
