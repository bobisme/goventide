package postgres_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/bobisme/goventide/postgres"
	"github.com/jackc/pgx/v4"
)

var _ = Describe("Reader", func() {
	var db *pgx.Conn
	var store *Store

	BeforeEach(func() {
		var err error
		for errCount := 0; errCount < 20; errCount++ {
			db, err = pgx.Connect(
				context.Background(),
				fmt.Sprintf(
					`postgres://postgres@localhost:%d/message_store`,
					eventideContainer.port))
			if err != nil {
				time.Sleep(500 * time.Millisecond)
			} else {
				break
			}
		}
		if err != nil {
			panic(err)
		}
		store = NewStore(db)
	})

	AfterEach(func() {
		store = nil
	})

	Describe("Category", func() {
		It("returns the category", func() {
			cat, err := store.Category(context.TODO(), "someEntity-12345")
			expectNo(err)
			Expect(cat).To(Equal("someEntity"))
		})

		It("returns fancy categories", func() {
			cat, err := store.Category(
				context.TODO(), "someEntity:blah+position,x-12345")
			expectNo(err)
			Expect(cat).To(Equal("someEntity:blah+position,x"))
		})
	})
})
