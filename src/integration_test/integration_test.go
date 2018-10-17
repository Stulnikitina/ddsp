package integration_test

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"time"

	"integration_test/runner"
	"storage"
	"testing"
)

const n = 100

var (
	router storage.ServiceAddr = "127.0.0.1:7320"

	nodes = []storage.ServiceAddr{
		"127.0.0.1:7321",
		"127.0.0.1:7322",
		"127.0.0.1:7323",
		"127.0.0.1:7324",
		"127.0.0.1:7325",
		"127.0.0.1:7326",
	}

	fe = []storage.ServiceAddr{
		"127.0.0.1:7318",
		"127.0.0.1:7319",
	}
)

func getTestData(key storage.RecordID) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(key))

	h := md5.New()
	return h.Sum(buf)
}

func iterationSimple(t *testing.T, n int) {
	client := storage.NewClient()

	keys := make([]storage.RecordID, 0, n)
	for k := 0; k < n; k++ {
		keys = append(keys, storage.RecordID(k))
	}

	for _, i := range rand.Perm(n) {
		key := keys[i]
		data := getTestData(key)

		ind := rand.Intn(len(fe))
		if err := client.Put(fe[ind], key, data); err != nil {
			t.Fatalf("Error putting record (key=%v, data=%v) via %v: %v", key, data, fe[ind], err)
		}
	}

	for _, i := range rand.Perm(n) {
		key := keys[i]
		ind := rand.Intn(len(fe))
		want := getTestData(key)
		got, err := client.Get(fe[ind], key)
		if err != nil {
			t.Fatalf("Error getting record (key=%v) via %v: %v", key, fe[ind], err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Wrong data: got %v, want %v", got, want)
		}
	}

	for _, i := range rand.Perm(n) {
		key := keys[i]
		data := getTestData(key)

		ind := rand.Intn(len(fe))
		if err := client.Put(fe[ind], key, data); err != storage.ErrRecordExists {
			t.Fatalf("Put() got error %v, want %v", err, storage.ErrRecordExists)
		}
	}

	for _, i := range rand.Perm(n) {
		key := keys[i]
		ind := rand.Intn(len(fe))
		if err := client.Del(fe[ind], key); err != nil {
			t.Fatalf("Error deleting record (key=%v) via %v: %v", key, fe[ind], err)
		}
	}

	for _, i := range rand.Perm(n) {
		key := keys[i]
		ind := rand.Intn(len(fe))
		if _, err := client.Get(fe[ind], key); err != storage.ErrRecordNotFound {
			t.Fatalf("Get() got error %v, want %v", err, storage.ErrRecordNotFound)
		}
	}

	for _, i := range rand.Perm(n) {
		key := keys[i]
		ind := rand.Intn(len(fe))
		if err := client.Del(fe[ind], key); err != storage.ErrRecordNotFound {
			t.Fatalf("Del() got error %v, want %v", err, storage.ErrRecordNotFound)
		}
	}
}

func TestAllAlive(t *testing.T) {
	r := &runner.Runner{}
	r.Start(router, fe, nodes, nodes)
	iterationSimple(t, n)
	r.Stop()
}

func TestOneDead(t *testing.T) {
	r := &runner.Runner{}

	for i := 0; i < len(nodes); i++ {
		alive := make([]storage.ServiceAddr, len(nodes)-1)
		copy(alive, nodes[:i])
		copy(alive[i:], nodes[i+1:])
		t.Run(fmt.Sprintf("alive=%v", alive), func(t *testing.T) {
			r.Start(router, fe, nodes, alive)
			iterationSimple(t, n)
			r.Stop()
		})
	}
}

func TestTwoDead(t *testing.T) {
	r := &runner.Runner{}

	client := storage.NewClient()
	nodesList := append(nodes, nodes...)
	for i := 0; i < len(nodes); i++ {
		alive := nodesList[i : i+len(nodes)-2]
		t.Run(fmt.Sprintf("alive=%v", alive), func(t *testing.T) {
			r.Start(router, fe, nodes, alive)

			keys := make([]storage.RecordID, 0, n)
			for k := 0; k < n; k++ {
				keys = append(keys, storage.RecordID(k))
			}
			failsPut := 0
			failsGet := 0
			failsDel := 0
			for _, i := range rand.Perm(n) {
				key := keys[i]
				data := getTestData(key)

				ind := rand.Intn(len(fe))
				if err := client.Put(fe[ind], key, data); err != nil {
					failsPut++
				}

				want := getTestData(key)
				got, err := client.Get(fe[ind], key)
				if err != nil {
					failsGet++
				} else if !bytes.Equal(got, want) {
					t.Fatalf("Wrong data: got %v, want %v", got, want)
				}
				if err := client.Del(fe[ind], key); err != nil {
					failsDel++
				}
			}
			r.Stop()
			if failsPut != failsGet || failsPut != failsDel {
				t.Fatalf("Expected equal number of fails for put, get, del ops, got put fails = %d, get fails = %d, del fails = %d ",
					failsPut, failsGet, failsDel,
				)
			}
			if failsPut > n/4 {
				t.Fatalf("Not more than %d fails expected, got %d fails", n/4, failsPut)
			}
		})
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
	log.SetOutput(ioutil.Discard)
	os.Exit(m.Run())
}
