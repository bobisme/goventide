package postgres

import (
	"context"
	"encoding/json"
	"log"

	"github.com/jackc/pgx/v4"
	uuid "github.com/satori/go.uuid"
)

func CanonizeExpectedVersion(v *int) *int {
	// return nil if expected_version.nil?
	// return expected_version unless expected_version == NoStream.name
	// NoStream.version
	return v
}

type put struct {
	db *pgx.Conn
}

func newPut(db *pgx.Conn) *put {
	return &put{db}
}

func (p *put) Put(
	writeMessage *Msg, streamName string,
	expectVer *int, session int) {
	//
	writeMessage.ID = uuid.NewV4().String()
	id, typ, data, meta := destructureMessage(writeMessage)
	expectVer = CanonizeExpectedVersion(expectVer)
	p.insertMessage(id, streamName, typ, data, meta, expectVer, func(pos int) {
		log.Printf("put message data %+v", data)
	})
}

func (p *put) insertMessage(
	id, streamName, typ string,
	data, meta interface{}, expectVer *int, callback func(pos int)) {
	//

	// transformed_data = transformed_data(data)
	// transformed_metadata = transformed_metadata(metadata)
	// records = execute_query(id, stream_name, type, transformed_data, transformed_metadata, expected_version)
	// position(records)
	jdata, err := json.Marshal(data)
	panicIf(err)
	jmeta, err := json.Marshal(meta)
	panicIf(err)
	log.Println("WRITING", id, streamName, typ, string(jdata), string(jmeta), expectVer)
	pos, err := p.executeQuery(id, streamName, typ, data, meta, expectVer)
	panicIf(err)
	callback(pos)
}

func (p *put) executeQuery(
	id, streamName, typ string, data, meta interface{}, expectVer *int) (int, error) {
	//
	statement := `
		SELECT write_message(
			$1::varchar, $2::varchar, $3::varchar,
			$4::jsonb, $5::jsonb, $6::bigint)`
	var position int
	err := p.db.QueryRow(context.TODO(), statement, id, streamName, typ, data, meta, expectVer).
		Scan(&position)
	if err != nil {
		return 0, err
	}
	return position, nil
}
