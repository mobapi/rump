package main

import (
	"flag"
	"fmt"
	"os"
	"github.com/gomodule/redigo/redis"
)

type Node struct {
	value string
	pttl int64
}

// Report all errors to stdout.
func handle(err error) {
	if err != nil && err != redis.ErrNil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// Scan and queue source keys.
func get(conn redis.Conn, queue chan<- map[string]Node) {
	var (
		cursor int64
		keys []string
	)

	for {
		// Scan a batch of keys.
		values, err := redis.Values(conn.Do("SCAN", cursor))
		handle(err)
		values, err = redis.Scan(values, &cursor, &keys)
		handle(err)

		// Get pipelined dumps.
		for _, key := range keys {
			conn.Send("DUMP", key)
		}
		dumps, err := redis.Strings(conn.Do(""))
		handle(err)

		// Get pipelined ttls.
		for _, key := range keys {
			conn.Send("PTTL", key)
		}
		pttls, err := redis.Int64s(conn.Do(""))
		handle(err)

		// Build batch map.
		batch := make(map[string]Node)
		for i, _ := range keys {
			batch[keys[i]] = Node {
				value: dumps[i],
				pttl: pttls[i],
			}
		}

		// Last iteration of scan.
		if cursor == 0 {
			// queue last batch.
			select {
			case queue <- batch:
			}
			close(queue)
			break
		}

		fmt.Printf(">")
		// queue current batch.
		queue <- batch
	}
}

// Restore a batch of keys on destination.
func put(conn redis.Conn, queue <-chan map[string]Node) {
	for batch := range queue {
		for key, node := range batch {
			conn.Send("RESTORE", key, node.pttl, node.value)
			// conn.Send("RESTORE", key, "0", node.value)
		}
		_, err := conn.Do("")
		handle(err)

		fmt.Printf(".")
	}
}

func main() {
	from := flag.String("from", "", "example: redis://127.0.0.1:6379/0")
	to := flag.String("to", "", "example: redis://127.0.0.1:6379/1")
	flag.Parse()

	source, err := redis.DialURL(*from)
	handle(err)
	destination, err := redis.DialURL(*to)
	handle(err)
	defer source.Close()
	defer destination.Close()

	// Channel where batches of keys will pass.
	queue := make(chan map[string]Node, 100)

	// Scan and send to queue.
	go get(source, queue)

	// Restore keys as they come into queue.
	put(destination, queue)

	fmt.Println("Sync done.")
}
