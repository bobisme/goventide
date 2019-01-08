package postgres

type Msg struct {
	ID       string
	Type     string
	Data     interface{}
	Metadata interface{}
}

func destructureMessage(msg *Msg) (id, typ string, data, meta interface{}) {
	if msg == nil {
		return
	}
	return msg.ID, msg.Type, msg.Data, msg.Metadata
}
