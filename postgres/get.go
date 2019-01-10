package postgres

import (
	"fmt"
	"log"
	"time"

	"github.com/bobisme/goventide/streamname"
	"github.com/jackc/pgx"
)

type get struct {
	db *pgx.Conn
}

func newGet(db *pgx.Conn) *get {
	return &get{db}
}

type ReadMsg struct {
	ID             string    `json:"id"`
	StreamName     string    `json:"stream_name"`
	Type           string    `json:"type"`
	Position       int       `json:"position"`
	GlobalPosition int       `json:"global_position"`
	Data           *string   `json:"data"`
	Metadata       *string   `json:"metadata"`
	Time           time.Time `json:"time"`
}

func (g *get) Get(
	streamName string, position int, batchSize int, condition *string) []ReadMsg {
	stmt := g.sqlCommand(streamName)
	cond := g.constrainCondition(condition)
	rows, err := g.db.Query(stmt, streamName, position, batchSize, cond)
	log.Println("GET", stmt, streamName, position, batchSize, cond)
	defer rows.Close()
	panicIf(err)
	messages := make([]ReadMsg, 0, batchSize)
	for rows.Next() {
		m := ReadMsg{}
		err := rows.Scan(
			&m.ID, &m.StreamName, &m.Type, &m.Position, &m.GlobalPosition,
			&m.Data, &m.Metadata, &m.Time)
		messages = append(messages, m)
		panicIf(err)
	}
	return messages
}

func (g *get) constrainCondition(cond *string) *string {
	if cond == nil {
		return nil
	}
	s := fmt.Sprintf("(%s)", *cond)
	return &s
}

func (g *get) sqlCommand(streamName string) string {
	params := `$1::varchar, $2::bigint, $3::bigint, $4::varchar`
	if g.isCategoryStream(streamName) {
		return fmt.Sprintf(`SELECT * FROM get_category_messages(%s)`, params)
	}
	return fmt.Sprintf(`SELECT * FROM get_stream_messages(%s)`, params)
}

func (g *get) isCategoryStream(streamName string) bool {
	return streamname.IsCategory(streamName)
}
