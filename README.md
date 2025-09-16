# icanal
A canal client for golang

## Usage
Simple Connector

> see example/simple.go
```go
    ctx := context.TODO()
	connector := icanal.NewSimpleConnector("127.0.0.1:11111", "example",
		icanal.WithUsername("canal"), icanal.WithPassword("canal"))

	if err := connector.Connect(ctx); err != nil {
		fmt.Printf("connect error: %v", err)
		return
	}
	if err := connector.Subscribe(ctx, ".*"); err != nil {
		fmt.Printf("subscribe error: %v", err)
		return
	}
	for {
		message, err := connector.Get(ctx, 10, 0)
		if err != nil {
			fmt.Printf("get error: %v", err)
			return
		}
		batchId := message.Id
		if batchId == -1 || len(message.Entries) <= 0 {
			time.Sleep(300 * time.Millisecond)
			fmt.Println("===没有数据了===")
			continue
		}

		printEntry(message.Entries)
	}
```