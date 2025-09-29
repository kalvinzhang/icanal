package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/kalvinzhang/icanal"
	"github.com/kalvinzhang/icanal/example/util"
)

func main() {
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

}
