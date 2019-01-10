package main

import (
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/bobisme/goventide/postgres"
	"github.com/bobisme/goventide/streamname"
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

func readerSvc(conf pgx.ConnConfig, consumerId int, totalConsumers int) {
	db, err := pgx.Connect(conf)
	defer db.Close()
	panicIf(err)
	s := postgres.NewStore(db)

	pos := 0
	batchSize := 10
	log.Printf("starting reader service %d", consumerId)
	STREAM_NAME := "nothing"

	sConsumerId := fmt.Sprintf("%d", consumerId)
	last := s.Last(streamname.StreamName("nothing", sConsumerId, "position"))
	if last != nil && last.Data != nil {
		posData := make(map[string]int)
		err := json.Unmarshal([]byte(*last.Data), &posData)
		panicIf(err)
		pos = posData["position"]
	}

	log.Println(consumerId, "reading string", STREAM_NAME, "from position", pos)
	for {
		msgs := s.Get(STREAM_NAME, pos, batchSize, nil)
		if len(msgs) == 0 {
			log.Println(consumerId, "no messages")
			time.Sleep(100 * time.Millisecond)
			continue
		} else {
			log.Println(consumerId, len(msgs), "messages")
		}
		for _, msg := range msgs {
			j, err := json.Marshal(msg)
			panicIf(err)
			log.Printf("%d read: %s", consumerId, string(j))
			if streamname.IsCategory(STREAM_NAME) {
				pos = msg.GlobalPosition
			} else {
				pos = msg.Position
			}
		}

		msg := new(postgres.Msg)
		msg.Type = "Read"
		objId := fmt.Sprintf("%d", consumerId)
		streamName := streamname.StreamName("nothing", objId, "position")
		msg.Data = map[string]int{
			"position": pos,
		}
		s.Put(msg, streamName, nil, 0)
	}
}

func producer(conf pgx.ConnConfig) {
	db, err := pgx.Connect(conf)
	defer db.Close()
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
	streamName := streamname.StreamName("nothing", objId)
	s.Put(msg, streamName, nil, 0)

}

func printMessages(conf pgx.ConnConfig) {
	db, err := pgx.Connect(conf)
	defer db.Close()
	panicIf(err)

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

func main() {
	// db, err := sql.Open("pgx", "host=127.0.0.1 database=message_store user=postgres")
	conf, err := pgx.ParseDSN("host=127.0.0.1 database=message_store user=postgres")
	panicIf(err)
	// db, err := pgx.Connect(conf)
	// panicIf(err)

	// s := postgres.NewStore(db)

	go readerSvc(conf, 0, 0)
	runtime.Gosched()
	// producer(conf)
	time.Sleep(600 * time.Millisecond)
	// printMessages(conf)
}
