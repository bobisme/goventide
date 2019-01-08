package postgres

import "github.com/jackc/pgx"

type Store struct {
	*put
}

func NewStore(db *pgx.Conn) *Store {
	return &Store{newPut(db)}
}
