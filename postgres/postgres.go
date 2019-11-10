package postgres

import (
	"context"

	"github.com/bobisme/goventide/store"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

const (
	categoryStmt      = "SELECT * FROM category($1::varchar)"
	streamVersionStmt = "SELECT * FROM stream_version($1::varchar)"
	writeMessageStmt  = `
		SELECT write_message(
			$1::varchar, $2::varchar, $3::varchar,
			$4::jsonb, $5::jsonb, $6::bigint)`
)

func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}

type OldStore struct {
	*put
	*get
}

func NewOldStore(db *pgx.Conn) *OldStore {
	return &OldStore{
		newPut(db),
		newGet(db),
	}
}

type DB interface {
	Exec(ctx context.Context, stmt string, params ...interface{}) (
		pgconn.CommandTag, error)
	QueryRow(ctx context.Context, stmt string, params ...interface{}) pgx.Row
}

type Store struct {
	db DB
}

func NewStore(db DB) *Store {
	return &Store{db}
}

func (s *Store) Category(ctx context.Context, streamName string) (string, error) {
	var c string
	if err := s.db.QueryRow(ctx, categoryStmt, streamName).Scan(&c); err != nil {
		return "", err
	}
	return c, nil
}

func (s *Store) StreamVersion(ctx context.Context, streamName string) (int, bool, error) {
	var v *int
	if err := s.db.QueryRow(ctx, streamVersionStmt, streamName).Scan(&v); err != nil {
		return 0, false, err
	}
	if v == nil {
		return 0, false, nil
	}
	return *v, true, nil
}

func (s *Store) WriteMessage(
	ctx context.Context, id, streamName, msgType string, data interface{},
	opts ...store.WriteMessageOpt,
) (int, error) {
	o := store.GetWriteMessageOptions(opts)
	var ver int
	var metadata interface{}
	err := s.db.QueryRow(
		ctx, writeMessageStmt, id, streamName, msgType, data, metadata,
		o.ExpectedVersion(),
	).Scan(&ver)
	if err != nil {
		return 0, err
	}
	return ver, nil
}
