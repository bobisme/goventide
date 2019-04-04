package streamname_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestStreamname(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Streamname Suite")
}
