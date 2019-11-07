package postgres

import "github.com/jackc/pgx/v4"

func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}

type Store struct {
	*put
	*get
}

func NewStore(db *pgx.Conn) *Store {
	return &Store{
		newPut(db),
		newGet(db),
	}
}
