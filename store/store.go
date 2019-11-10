package store

import "context"

type Reader interface {
	Category(ctx context.Context, streamName string) (string, error)
	StreamVersion(ctx context.Context, streamName string) (v int, ok bool, err error)
}

type WriteMessageOptions struct {
	expectedVer *int
}

// ExpectedVersion returns the expected version of the stream or nil.
func (o WriteMessageOptions) ExpectedVersion() *int {
	return o.expectedVer
}

type WriteMessageOpt func(o *WriteMessageOptions)

func ExpectVersion(v int) WriteMessageOpt {
	return func(o *WriteMessageOptions) {
		o.expectedVer = &v
	}
}

func GetWriteMessageOptions(opts []WriteMessageOpt) WriteMessageOptions {
	o := WriteMessageOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

type Writer interface {
	WriteMessage(
		ctx context.Context, id, streamName, msgType string, data interface{},
		opts ...WriteMessageOpt,
	) (int, error)
}

type Store interface {
	Reader
	Writer
}
