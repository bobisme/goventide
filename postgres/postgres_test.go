package postgres_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"contrib.go.opencensus.io/exporter/prometheus"
	. "github.com/bobisme/goventide/postgres"
	"github.com/bobisme/goventide/store"
	"github.com/bobisme/goventide/store/instrument"
	"github.com/jackc/pgx/v4"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rs/zerolog/log"
	uuid "github.com/satori/go.uuid"
	"go.opentelemetry.io/otel/api/core"
	"go.opentelemetry.io/otel/api/key"
	"go.opentelemetry.io/otel/exporter/trace/jaeger"
	"go.opentelemetry.io/otel/global"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var (
	someUUIDs = []string{
		"96d771e0-ae7f-456a-9f46-9a7fd789ec3f",
		"653339ef-26cd-49a6-b98c-4b88890e0612",
		"181c6cf0-a966-490d-ae1d-7069a172e563",
		"9db28fa7-ff61-4be0-aa72-715c72d2b3e8",
		"de62f915-3419-4778-ba77-ef10b1d3c92c",
		"9594aaec-a9f6-4882-8f0f-5ffd4c777bfb",
		"1e11665b-da52-4a4f-90b5-cbb86845ae6a",
		"a566611c-503f-447d-b4b8-e4fed152b908",
	}
)

func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}

func newUUID() string {
	return uuid.NewV4().String()
}

func mustWrite(v int, err error) int {
	panicIf(err)
	return v
}

// todo just returns context.TODO()
func todo() context.Context {
	return context.TODO()
}

func connectToDB() *pgx.Conn {
	var err error
	var db *pgx.Conn
	for errCount := 0; errCount < 100; errCount++ {
		dbURI := fmt.Sprintf(
			`postgres://postgres@localhost:%d/message_store`,
			eventideContainer.port)
		db, err = pgx.Connect(context.Background(), dbURI)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		panic(err)
	}
	return db
}

func initTracer() func() {
	// exporter, err := stdout.NewExporter(stdout.Options{PrettyPrint: true})
	exporter, err := jaeger.NewExporter(
		jaeger.WithCollectorEndpoint("http://localhost:14268/api/traces"),
		jaeger.WithProcess(jaeger.Process{
			ServiceName: "trace-demo",
			Tags: []core.KeyValue{
				key.String("exporter", "jaeger"),
				key.Float64("float", 312.23),
			},
		}),
	)
	if err != nil {
		panic(err)
	}
	tp, err := sdktrace.NewProvider(
		sdktrace.WithConfig(
			sdktrace.Config{DefaultSampler: sdktrace.AlwaysSample()}),
		sdktrace.WithSyncer(exporter))
	if err != nil {
		panic(err)
	}
	global.SetTraceProvider(tp)

	pe, err := prometheus.NewExporter(prometheus.Options{
		Namespace: "poopscoop",
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create the Prometheus  exporter")
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", pe)
	go func() {
		if err := http.ListenAndServe(":30088", mux); err != nil {
			log.Fatal().Err(err).Msg("Failed to run Prometheus scrape endpoint")
		}
	}()

	return func() {
		println("flushing exporter")
		exporter.Flush()
		println("printing stats")
		resp, err := http.Get("http://localhost:30088/metrics")
		if err != nil {
			log.Error().Err(err).Msg("could not read metrics")
		}
		io.Copy(os.Stdout, resp.Body)
	}
}

var _ = Describe("Reader", func() {
	var flushTraceExporter func()
	var db *pgx.Conn
	var s store.Store

	BeforeSuite(func() {
		flushTraceExporter = initTracer()
	})

	AfterSuite(func() {
		flushTraceExporter()
	})

	BeforeEach(func() {
		if s == nil {
			db = connectToDB()
			s = instrument.InstrumentStore(NewStore(NewTracingDB(db)))
		}
		_, err := db.Exec(todo(), "TRUNCATE messages")
		if err != nil {
			panic(err)
		}
	})

	Describe("Category", func() {
		It("returns the category", func() {
			cat, err := s.Category(todo(), "someEntity-12345")
			expectNo(err)
			Expect(cat).To(Equal("someEntity"))
		})

		It("returns fancy categories", func() {
			cat, err := s.Category(
				todo(), "someEntity:blah+position,x-12345")
			expectNo(err)
			Expect(cat).To(Equal("someEntity:blah+position,x"))
		})
	})

	Describe("StreamVersion", func() {
		It("is not ok for a missing stream", func() {
			_, ok, err := s.StreamVersion(todo(), "entity-01234")
			expectNo(err)
			Expect(ok).To(BeFalse())
		})

		It("returns the version of the stream", func() {
			data := map[string]bool{"isCool": true}
			for i := 0; i < 3; i++ {
				id := someUUIDs[i]
				_, err := s.WriteMessage(
					todo(), id, "entity-01234", "MessageWritten", data)
				expectNo(err)

				v, ok, err := s.StreamVersion(todo(), "entity-01234")
				expectNo(err)
				Expect(ok).To(BeTrue())
				Expect(v).To(Equal(i))
			}
		})
	})

	Describe("WriteMessage", func() {
		data := map[string]bool{"isCool": true}

		Context("writing a message", func() {
			var prevGlobalPos int

			var out struct {
				id         string
				streamName string
				msgType    string
				ver        int
				globalPos  int
				data       string
				metadata   *string
				at         time.Time
			}

			BeforeEach(func() {
				var prevGlobalPosOut *int
				err := db.QueryRow(
					todo(), `SELECT max(global_position) FROM messages`,
				).Scan(&prevGlobalPosOut)
				panicIf(err)
				if prevGlobalPosOut != nil {
					prevGlobalPos = *prevGlobalPosOut
				}
				_, err = s.WriteMessage(
					todo(), someUUIDs[0], "entity-01234", "MessageWritten", data)
				panicIf(err)

				err = db.QueryRow(todo(), `SELECT * FROM messages`).Scan(
					&out.id, &out.streamName, &out.msgType, &out.ver,
					&out.globalPos, &out.data, &out.metadata, &out.at,
				)
				panicIf(err)
			})

			It("stores the id", func() {
				Expect(out.id).To(Equal("96d771e0-ae7f-456a-9f46-9a7fd789ec3f"))
			})
			It("stores the stream name", func() {
				Expect(out.streamName).To(Equal("entity-01234"))
			})
			It("stores the message type", func() {
				Expect(out.msgType).To(Equal("MessageWritten"))
			})
			It("stores the stream version", func() {
				Expect(out.ver).To(Equal(0))
			})
			It("stores the global position", func() {
				Expect(out.globalPos).To(Equal(prevGlobalPos + 1))
			})
			It("stores the data", func() {
				Expect(out.data).To(Equal(`{"isCool": true}`))
			})
			// It("stores the time the message was recorded", func() {})
		})

		It("returns the version of the stream", func() {
			// Verify the version of this stream is still 0
			ver, err := s.WriteMessage(
				todo(), someUUIDs[2], "entity-01234", "MessageWritten", data)
			expectNo(err)
			Expect(ver).To(Equal(0))
		})

		Context("when there are messages in other streams", func() {
			BeforeEach(func() {
				mustWrite(s.WriteMessage(
					todo(), newUUID(), "junk-01234", "Garbage", data))
				mustWrite(s.WriteMessage(
					todo(), newUUID(), "junk-01234", "Garbage", data))
			})

			It("only returns the version of the stream being written to", func() {
				// Verify the version of this stream is still 0
				ver, err := s.WriteMessage(
					todo(), someUUIDs[2], "entity-01234", "MessageWritten", data)
				expectNo(err)
				Expect(ver).To(Equal(0))
			})
		})

		It("writes metadata", func() {

		})

		It("writes a message if the version of the stream is as expected", func() {

		})

		XContext("the expected version of a stream does not match", func() {
			var err error

			BeforeEach(func() {
				_, err = s.WriteMessage(
					todo(), someUUIDs[0], "entity-1234", "Hello", data,
					store.ExpectVersion(42))
			})

			It("throws an error ", func() {
				Expect(err).NotTo(BeNil())
			})

			It("does not write the message to the stream", func() {
				count := -1
				err = db.QueryRow(
					todo(), `SELECT count(*) FROM messages`,
				).Scan(&count)
				expectNo(err)
				Expect(count).To(Equal(0))
			})
		})

		// _id varchar,
		// _stream_name varchar,
		// _type varchar,
		// _data jsonb,
		// _metadata jsonb DEFAULT NULL,
		// _expected_version bigint DEFAULT NULL
	})
})
