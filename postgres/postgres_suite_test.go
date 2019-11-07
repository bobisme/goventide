package postgres_test

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

type container struct {
	name string
	port int
}

var (
	eventideContainer *container
)

func expectNo(err error) {
	Expect(err).To(BeNil())
}

func (c *container) stop() {
	fmt.Println("stopping container", c.name)
	cmd := exec.Command("docker rm -rf " + c.name)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	cmd.Run()
}

func randStr(chars int) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	out := make([]byte, chars)
	for i := 0; i < chars; i++ {
		out[i] = charset[r.Intn(len(charset))]
	}
	return string(out)
}

func startContainer() *container {
	fmt.Println("Starting container")
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	containerName := "eventide-test-" + randStr(12)
	port := 30000 + r.Intn(10000)
	cmd := exec.Command(
		"docker", "run", "--rm", "-d", "--name", containerName, "-p",
		fmt.Sprintf("%d:5432", port), "bobisme/eventide",
	)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	err := cmd.Run()
	if err != nil {
		panic(err)
	}

	fmt.Println("Started container", containerName, "on port", port)
	return &container{containerName, port}
}

func TestPostgres(t *testing.T) {
	eventideContainer = startContainer()
	defer eventideContainer.stop()
	RegisterFailHandler(Fail)
	RunSpecs(t, "Postgres Suite")
}
