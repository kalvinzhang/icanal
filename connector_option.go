package icanal

import "time"

const (
	BatchSizeDefault     = 1000                  // 默认batch size
	ClientIdDefault      = 1001                  // 默认client id
	SoTimeoutDefault     = 60 * time.Second      // 默认socket超时；60秒
	IdleTimeoutDefault   = 60 * 60 * time.Second // 默认空闲超时；60分钟
	RetryTimesDefault    = 3                     // 默认重试次数
	RetryIntervalDefault = 5 * time.Second       // 默认重试间隔时间
)

// ConnectorConfig 简单客户端配置
type ConnectorConfig struct {
	Username             string        // 用户名
	Password             string        // 密码
	SoTimeout            time.Duration // 网络超时
	IdleTimeout          time.Duration // 空闲超时
	RollbackOnConnect    bool          // 是否在connect链接成功后，自动执行rollback操作
	RollbackOnDisconnect bool          // 是否在connect链接断开后，自动执行rollback操作
	LazyParseEntry       bool          // 是否自动化解析Entry对象,如果考虑最大化性能可以延后解析
	Filter               string        // 记录上一次的filter提交值,便于自动重试时提交
	RetryTimes           int
	RetryInterval        time.Duration
}

func getDefaultConfig() *ConnectorConfig {
	return &ConnectorConfig{
		SoTimeout:            SoTimeoutDefault,
		IdleTimeout:          IdleTimeoutDefault,
		RollbackOnConnect:    true,
		RollbackOnDisconnect: false,
		LazyParseEntry:       false,
		RetryTimes:           RetryTimesDefault,
		RetryInterval:        RetryIntervalDefault,
	}
}

type Option func(*ConnectorConfig)

func WithUsername(username string) Option {
	return func(c *ConnectorConfig) {
		c.Username = username
	}
}

func WithPassword(password string) Option {
	return func(c *ConnectorConfig) {
		c.Password = password
	}
}

func WithIdleTimeout(idleTimeout time.Duration) Option {
	return func(c *ConnectorConfig) {
		c.IdleTimeout = idleTimeout
	}
}

func WithSoTimeout(soTimeout time.Duration) Option {
	return func(c *ConnectorConfig) {
		c.SoTimeout = soTimeout
	}
}

func WithFilter(filter string) Option {
	return func(c *ConnectorConfig) {
		c.Filter = filter
	}
}

func WithRollbackOnConnect(rollbackOnConnect bool) Option {
	return func(c *ConnectorConfig) {
		c.RollbackOnConnect = rollbackOnConnect
	}
}

func WithRollbackOnDisconnect(rollbackOnDisconnect bool) Option {
	return func(c *ConnectorConfig) {
		c.RollbackOnDisconnect = rollbackOnDisconnect
	}
}

func WithLazyParseEntry(lazyParseEntry bool) Option {
	return func(c *ConnectorConfig) {
		c.LazyParseEntry = lazyParseEntry
	}
}

func WithRetryTimes(retryTimes int) Option {
	return func(c *ConnectorConfig) {
		c.RetryTimes = retryTimes
	}
}

func WithRetryInterval(retryInterval time.Duration) Option {
	return func(c *ConnectorConfig) {
		c.RetryInterval = retryInterval
	}
}
