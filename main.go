package main

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/antoni-ostrowski/mq/internal/broker"
	"github.com/antoni-ostrowski/mq/internal/topic"
)

//               PUBLISH
// Producer ──────────────┐
// Producer ──────────────┤
//                        ▼
//                  ┌─────────────┐
//                  │    QUEUE    │
//                  │             │
//                  │  ┌───────┐  │  publish appends here
//                  │  │ ready │◄─┘
//                  │  │ [m3]  │
//                  │  │ [m2]  │
//                  │  │ [m1]  │
//                  │  └───────┘  │
//                  │       │ dispatch (pop front, non-blocking)
//                  │       ▼     │
//                  │  ┌────────┐ │
//                  │  │unacked │ │  waiting for ACK
//                  │  │m0->A   │ │
//                  │  │m-1->B   │ │
//                  │  └────────┘ │
//                  └─────────────┘
//                    │         │
//          ┌─────────┘         └─────────┐
//          ▼                             ▼
//    ┌───────────┐                 ┌───────────┐
//    │Consumer A │                 │Consumer B │
//    │prefetch:  │                 │prefetch:  │
//    │ 2 unacked │                 │ 0 unacked │
//    └───────────┘                 └───────────┘
//          │                             │
//          │ ACK  -> delete from unacked  │
//          │ NACK / disconnect -> back to front of ready
//          └─────────────────────────────┘
//
// Message lifecycle, one message:
//
// publish -> [ready] --dispatch--> [unacked, owner=A] --ACK--> deleted
//                                       |
//                                       +--NACK--> front of [ready]
//                                       +--disconnect(A)--> front of [ready]
//

func main() {
	l, err := net.Listen("tcp", ":3000")
	if err != nil {
		slog.Error("failed to start tcp listener", "error", err.Error())
		return
	}
	slog.Info("listening for TCP on :3000")

	broker := broker.New()
	var wg sync.WaitGroup
	for {
		conn, err := l.Accept()
		if err != nil {
			slog.Error("failed to start accepting tcp conns", "error", err.Error())
			return
		}
		wg.Go(func() { handleConn(conn, broker) })
	}

}

func handleConn(conn net.Conn, broker *broker.Broker) {
	var wg sync.WaitGroup
	defer conn.Close()

	sub := topic.NewSubscriber(conn)

	defer broker.UnsubAll(sub)

	w := bufio.NewWriter(conn)
	wg.Go(func() {
		// MSG <topic> <id> <len>\nbytes
		for msg := range sub.Outbound {
			l := len(msg.Payload)
			if _, err := fmt.Fprintf(w, "MSG %s %d %d\n", msg.Topic, msg.ID, l); err != nil {
				conn.Close()
				return
			}
			if _, err := w.Write(msg.Payload); err != nil {
				conn.Close()
				return
			}
			if err := w.Flush(); err != nil {
				conn.Close()
				return
			}
		}
	})

	wg.Go(func() {
		defer close(sub.Outbound)
		r := bufio.NewReader(conn)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					slog.Error("failed to read from conn", "error", err.Error())
				}
				return
			}
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			args := strings.Fields(line)

			slog.Info("found client input")
			for _, arg := range args {
				slog.Info("arg", "arg", arg)
			}

			if len(args) == 0 {
				continue
			}
			cmd := args[0]

			switch cmd {
			case "SUB", "UNSUB":
				if len(args) < 2 {
					slog.Info("args slice is too short, want 2, got <2")
					continue
				}
				topicName := args[1]
				slog.Info("sub command detected", "topic", topicName)
				topic, err := broker.GetTopic(topicName)
				if err != nil {
					slog.Error("failed to get topic", "error", err.Error())
					continue
				}
				if cmd == "SUB" {
					topic.Sub(sub)
				} else {
					topic.Unsub(sub)
				}
			case "CREATE":
				if len(args) < 2 {
					slog.Info("args slice is too short, want 2, got <2")
					continue
				}
				newTopicName := args[1]
				broker.RegisterTopic(topic.New(newTopicName))
				// PUB <topic> <len>\nbytes
				// followed by exactly <len> raw bytes (may contain spaces/newlines)
			case "PUB":
				if len(args) < 3 {
					slog.Info("not enough args, need: PUB <topic> <len>")
					continue
				}
				t, err := broker.GetTopic(args[1])
				if err != nil {
					continue
				}
				payloadLen, err := strconv.Atoi(args[2])
				if err != nil || payloadLen < 0 {
					slog.Info("invalid payload len", "value", args[2])
					continue
				}
				const maxPayload = 1 << 20
				if payloadLen > maxPayload {
					slog.Info("payload too large", "len", payloadLen)
					continue
				}
				payloadBuf := make([]byte, payloadLen)
				if _, err := io.ReadFull(r, payloadBuf); err != nil {
					slog.Error("failed to read payload", "error", err.Error())
					return
				}

				t.Publish(topic.Message{ID: broker.GetNewMsgId(), Payload: payloadBuf})

			// ACK <topic> <id>
			case "ACK", "NACK":
				if len(args) < 3 {
					slog.Info("not enough args, need: ACK <topic> <id>")
					continue
				}

				t, err := broker.GetTopic(args[1])
				if err != nil {
					continue
				}
				id, err := strconv.ParseUint(args[2], 10, 64)
				if err != nil {
					continue
				}
				if cmd == "ACK" {
					t.Ack(sub, id)
				} else {
					t.Nack(sub, id)

				}

			}

		}

	})

	wg.Wait()
}
