package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/bobisme/goventide/postgres"
	"github.com/jackc/pgx"
	_ "github.com/jackc/pgx/stdlib"
	uuid "github.com/satori/go.uuid"
)

func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}

type Obj struct {
	ID     string `json:"id"`
	Hey    string `json:"hey"`
	Number int    `json:"number"`
}

func main() {
	// db, err := sql.Open("pgx", "host=127.0.0.1 database=message_store user=postgres")
	conf, err := pgx.ParseDSN("host=127.0.0.1 database=message_store user=postgres")
	panicIf(err)
	db, err := pgx.Connect(conf)
	panicIf(err)

	s := postgres.NewStore(db)

	msg := new(postgres.Msg)
	msg.Type = "CreateNothing"
	objId := uuid.NewV4().String()
	msg.Data = Obj{
		ID:     objId,
		Hey:    "there",
		Number: 42,
	}
	streamName := fmt.Sprintf("nothing-%s", objId)
	s.Put(msg, streamName, nil, 0)

	rows, err := db.Query(`SELECT * FROM messages`)
	panicIf(err)
	for rows.Next() {
		var x struct {
			Id, StreamName, Typ      string
			Position, GlobalPosition int
			Data, Meta               map[string]interface{}
			Time                     time.Time
		}
		err = rows.Scan(&x.Id, &x.StreamName, &x.Typ, &x.Position, &x.GlobalPosition,
			&x.Data, &x.Meta, &x.Time)
		panicIf(err)
		j, err := json.Marshal(x)
		panicIf(err)
		fmt.Println(string(j))
	}
	panicIf(rows.Err())
}
