package util

import (
	"context"
	"log/slog"

	"google.golang.org/protobuf/proto"

	"github.com/kalvinzhang/icanal"
)

func PrintEntry(ctx context.Context, entries []*icanal.Entry) {

	for _, entry := range entries {
		if entry.GetEntryType() == icanal.EntryType_TRANSACTIONBEGIN ||
			entry.GetEntryType() == icanal.EntryType_TRANSACTIONEND {
			continue
		}

		rowChange := &icanal.RowChange{}
		if err := proto.Unmarshal(entry.GetStoreValue(), rowChange); err != nil {
			slog.ErrorContext(ctx, "unmarshal error", slog.Any("error", err))
			continue
		}
		if rowChange != nil {
			eventType := rowChange.GetEventType()
			header := entry.GetHeader()
			slog.InfoContext(ctx, "binlog event",
				slog.String("logfileName", header.GetLogfileName()),
				slog.Int64("logfileOffset", header.GetLogfileOffset()),
				slog.String("schema", header.GetSchemaName()),
				slog.String("tableName", header.GetTableName()),
				slog.String("eventType", eventType.String()))

			for _, rowData := range rowChange.GetRowDatas() {
				if eventType == icanal.EventType_DELETE {
					printColumn(ctx, rowData.GetBeforeColumns())
				} else if eventType == icanal.EventType_INSERT {
					printColumn(ctx, rowData.GetAfterColumns())
				} else {
					slog.InfoContext(ctx, "before--->")
					printColumn(ctx, rowData.GetBeforeColumns())
					slog.InfoContext(ctx, "after--->")
					printColumn(ctx, rowData.GetAfterColumns())
				}
			}
		}
	}
}

func printColumn(ctx context.Context, columns []*icanal.Column) {
	for _, col := range columns {
		slog.InfoContext(ctx, "column info",
			slog.String("name", col.GetName()),
			slog.String("value", col.GetValue()),
			slog.Bool("updated", col.GetUpdated()),
		)
	}
}
