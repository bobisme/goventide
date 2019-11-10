package postgres

import (
	"context"
	"strings"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"go.opentelemetry.io/otel/api/key"
	"go.opentelemetry.io/otel/api/trace"
	"go.opentelemetry.io/otel/global"
)

var (
	queryKey = key.New("query")
)

type TracingDB struct {
	db DB
}

func NewTracingDB(db DB) *TracingDB {
	return &TracingDB{db}
}

func (t *TracingDB) Exec(
	ctx context.Context, query string, params ...interface{},
) (pgconn.CommandTag, error) {
	tr := global.TraceProvider().GetTracer("pgx")
	var tag pgconn.CommandTag
	var execErr error
	err := tr.WithSpan(ctx, "Exec", func(ctx context.Context) error {
		trace.CurrentSpan(ctx).SetAttributes(queryKey.String(
			strings.TrimSpace(query)))
		tag, execErr = t.db.Exec(ctx, query, params...)
		return nil
	})
	if err != nil {
		println("ERROR", err)
		return tag, err
	}
	return tag, execErr
}

func (t *TracingDB) QueryRow(ctx context.Context, query string, params ...interface{}) pgx.Row {
	tr := global.TraceProvider().GetTracer("pgx")
	var row pgx.Row
	err := tr.WithSpan(ctx, "QueryRow", func(ctx context.Context) error {
		trace.CurrentSpan(ctx).SetAttributes(queryKey.String(
			strings.TrimSpace(query)))
		row = t.db.QueryRow(ctx, query, params...)
		return nil
	})
	if err != nil {
		println("ERROR", err)
	}
	return row
}
