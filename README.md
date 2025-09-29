# icanal
A canal client for golang; 

## 说明
icanal是一个[alibaba canal](https://github.com/alibaba/canal)的golang client，灵感来源于[go-canal](https://github.com/withlin/canal-go)；但是比canal-go要更稳定和面像地道的golang设计，支持更灵活的配置化


## 使用
Simple Connector

> 参照 [example/simple.go](https://github.com/kalvinzhang/icanal/blob/main/example/simple.go)
```go
    ctx := context.TODO()
	connector := icanal.NewSimpleConnector(
		"127.0.0.1:11111", "example",
		icanal.WithUsername("canal"),
		icanal.WithPassword("canal"),
	)

	if err := connector.Connect(ctx); err != nil {
		slog.ErrorContext(ctx, "connect error", slog.Any("error", err))
		return
	}

	if err := connector.Subscribe(ctx, ".*\\..*"); err != nil {
		slog.ErrorContext(ctx, "subscribe error", slog.Any("error", err))
		return
	}
	for {
		message, err := connector.Get(ctx, 10, time.Second)
		if err != nil {
			slog.ErrorContext(ctx, "get error", slog.Any("error", err))
			return
		}
		batchId := message.Id
		if batchId == -1 || len(message.Entries) <= 0 {
			time.Sleep(300 * time.Millisecond)
			slog.DebugContext(ctx, "no data")
			continue
		}

		util.PrintEntry(ctx, message.Entries)
	}
```