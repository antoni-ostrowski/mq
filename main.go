package main

import (
	"log/slog"
	"net"
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

	wg.Go(func() {
		// for _ := range sub.Outbound {
		// 	//			conn.Write(msg.Payload)
		// }
	})

	wg.Go(func() {
		buf := make([]byte, 256)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				slog.Error("failed to read from conn", "error", err.Error())
				return
			}
			r, _, _ := strings.CutLast(string(buf[:n]), "\r\n")
			args := strings.Split(r, " ")

			slog.Info("found client input")
			for _, arg := range args {
				slog.Info("arg", "arg", arg)
			}

			if len(args) == 0 {
				slog.Info("args slice is 0 len")
				return
			}
			switch args[0] {
			case "SUB":
				if len(args) < 2 {
					slog.Info("args slice is too short, want 2, got <2")
					continue
				}
				topicName := args[1]
				slog.Info("sub command detected", "topic", topicName)
				topic, err := broker.GetTopic(topicName)
				if err != nil {
					slog.Error("failed to get topic", "error", err.Error())
					return
				}
				topic.Subscribe(sub)

			}
		}

	})

	wg.Wait()
}
