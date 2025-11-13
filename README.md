# icanal
A canal client for golang; 

## 说明
icanal是一个[alibaba canal](https://github.com/alibaba/canal)的golang client，灵感来源于[go-canal](https://github.com/withlin/canal-go)；但是比canal-go要更稳定和面像地道的golang设计，支持更灵活的配置化

## 安装
```
go get github.com/kalvinzhang/icanal
```

## 使用
### Cluster Connector

> 参照 [example/cluster/cluster.go](https://github.com/kalvinzhang/icanal/blob/main/example/cluster/cluster.go)

```go
	ctx := context.TODO()
	// 创建一个通道来接收信号
	sigChan := make(chan os.Signal, 1)
	// 使用signal.Notify注册要接收的信号，这里注册了SIGINT（Ctrl+C）
	signal.Notify(sigChan, syscall.SIGINT)

	connector := icanal.NewClusterConnector(
		"example",
		[]string{"127.0.0.1:2181"},
		time.Second*10,
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

outer:
	for {
		select {
		case <-sigChan:
			fmt.Println("Received an interrupt, stopping example...")
			break outer
		default:
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
	}

	if err := connector.Unsubscribe(ctx); err != nil {
		return
	}

	fmt.Println("unsubscribed")

	if err := connector.Disconnect(ctx); err != nil {
		return
	}

	fmt.Println("disconnected")

	fmt.Println("exited")
```


### Simple Connector

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