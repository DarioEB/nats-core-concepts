package nats_client

import (
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

type NatsClient struct {
	conn *nats.Conn
}

type JetStreamClient struct {
	nats.JetStreamContext
}

type SubscribeReply struct {
	*nats.Subscription
}

func NewConnection(host, port, user, pass string) (*NatsClient, error) {
	nc, err := nats.Connect(
		fmt.Sprintf("%s:%s", host, port),
		nats.UserInfo(user, pass),
		nats.Name("API Options Example"),
		nats.Timeout(10*time.Second),
	)

	if err != nil {
		return nil, err
	}

	return &NatsClient{conn: nc}, nil
}

func (nc NatsClient) CreateSubscribeReply(subject string, message string) *SubscribeReply {
	sub, _ := nc.conn.Subscribe(subject, func(msg *nats.Msg) {
		msg.Respond([]byte(message))
	})

	return &SubscribeReply{sub}
}

func (sub SubscribeReply) UnsubscribeReply() {
	fmt.Println("Unsubscribe...")
	sub.Unsubscribe()
}

func (nc NatsClient) CloseConnection() {
	nc.conn.Drain()
}

func (nc NatsClient) ListServers() []string {
	if nc.conn != nil {
		return nc.conn.Servers()
	}
	return []string{}
}

func (nc NatsClient) Subscription(subject string) (*nats.Subscription, error) {
	sub, err := nc.conn.SubscribeSync(subject)
	if err != nil {
		return nil, err
	}

	return sub, nil
}

func (nc NatsClient) PublishMessage(subject string, message string) {
	if err := nc.conn.Publish(subject, []byte(message)); err != nil {
		log.Fatal(err)
	}
}

func (nc NatsClient) CreateRequest(subject string) (reply *nats.Msg, err error) {
	rep, err := nc.conn.Request(subject, nil, time.Second)
	return rep, err
}

func (nc NatsClient) NewJetStream() *JetStreamClient {
	js, _ := nc.conn.JetStream()

	return &JetStreamClient{js}
}

func (nc NatsClient) NewSubject() string {
	uniqueSubject := nc.conn.NewInbox()
	fmt.Println("Unique reply to created: ", uniqueSubject)
	return uniqueSubject
}
