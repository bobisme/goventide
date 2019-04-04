package streamname_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	"github.com/bobisme/goventide/streamname"
)

var _ = Describe("StreamName", func() {
	It("combines category and id", func() {
		Expect(streamname.StreamName("cat", "id1234")).
			To(Equal("cat-id1234"))
	})

	It("includes the stream type", func() {
		Expect(streamname.StreamName("cat", "id1234", "typ1")).
			To(Equal("cat:typ1-id1234"))
	})

	It("includes multiple types", func() {
		Expect(streamname.StreamName("cat", "id1234", "typ1", "typ2")).
			To(Equal("cat:typ1+typ2-id1234"))
	})
})

var _ = Describe("ID", func() {
	It("returns the id of the stream", func() {
		Expect(streamname.ID("cat-1234")).To(Equal("1234"))
	})

	It("returns empty string if it can't get the id", func() {
		Expect(streamname.ID("cat:command")).To(Equal(""))
	})
})

var _ = Describe("Category", func() {
	DescribeTable("it returns the category from the stream",
		func(sn string, expected string) {
			Expect(streamname.Category(sn)).To(Equal(expected))
		},
		Entry("simple entity stream", "cat-1234", "cat"),
		Entry("stream type", "cat:typ1-1234", "cat:typ1"),
		Entry("multi type", "cat:t1+t2-1234", "cat:t1+t2"),
	)
})

var _ = Describe("IsCategory", func() {
	DescribeTable("it returns true if the stream name is just a category",
		func(sn string) { Expect(streamname.IsCategory(sn)).To(BeTrue()) },
		Entry("simple entity stream", "cat"),
		Entry("stream type", "cat:typ1"),
		Entry("multi type", "cat:t1+t2"),
	)

	DescribeTable("it returns false if the stream name is not just a category",
		func(sn string) { Expect(streamname.IsCategory(sn)).To(BeFalse()) },
		Entry("simple entity stream", "cat-1234"),
		Entry("stream type", "cat:typ1-1234"),
		Entry("multi type", "cat:t1+t2-abcd"),
	)
})

var _ = Describe("TypeList", func() {
	DescribeTable("it returns the type portion of the stream name",
		func(sn string, expected string) {
			Expect(streamname.TypeList(sn)).To(Equal(expected))
		},
		Entry("one type", "cat:typ1-1234", "typ1"),
		Entry("multiple types", "cat:typ1+typ2-1234", "typ1+typ2"),
		Entry("no types", "cat-1234", ""),
	)
})

var _ = Describe("Types", func() {
	DescribeTable("it returns the types from the stream name",
		func(sn string, expected []string) {
			Expect(streamname.Types(sn)).To(Equal(expected))
		},
		Entry("one type", "cat:typ1-1234", []string{"typ1"}),
		Entry("multiple types", "cat:typ1+typ2-1234", []string{"typ1", "typ2"}),
	)

	It("returns nil if there are no types on the stream name", func() {
		Expect(streamname.Types("cat-1234")).To(BeNil())
	})
})

var _ = Describe("EntityName", func() {
	DescribeTable("it returns only the entity name from the stream name",
		func(sn string, expected string) {
			Expect(streamname.EntityName(sn)).To(Equal(expected))
		},
		Entry("one type", "cat:typ1-1234", "cat"),
		Entry("multiple types", "cat:typ1+typ2-1234", "cat"),
		Entry("no types", "cat-1234", "cat"),
		Entry("category", "cat", "cat"),
	)
})
