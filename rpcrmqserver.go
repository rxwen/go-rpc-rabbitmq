package rpcrmq

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

type RPCService struct {
	Callback func([]byte) ([]byte, error)
}

var gRegisteredRPC map[string]*RPCService = make(map[string]*RPCService)

func RegisterRPC(rpcName string, svc *RPCService) {
	gRegisteredRPC[rpcName] = svc
}

func ensureServerQueue(ch *amqp.Channel, method string) (amqp.Queue, error) {
	q, err := ch.QueueDeclare(
		method, // name
		false,  // durable
		true,   // autoDelete
		false,  // exclusive
		false,  // noWait
		nil,    // args
	)
	if err != nil {
		log.Print("failed to declare request queue to make sure it exists", err)
		return q, err
	}
	return q, err
}

func startRPC(endpoint, method string, svc *RPCService) {
	con, err := amqp.Dial(endpoint)
	if err != nil {
		log.Print("failed to connect ", err)
		return
	}
	defer con.Close()

	ch, err := con.Channel()
	if err != nil {
		log.Print("failed to open a channel for method: ", method, err)
		return
	}
	defer ch.Close()

	q, err := ensureServerQueue(ch, method)
	if err != nil {
		log.Fatal("failed to declare queue ", method, err)
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // autoAck
		false,  // exclusive
		false,  // noLocal
		false,  // noWait
		nil,    // args
	)
	for d := range msgs {
		log.Print("got request ", d.ReplyTo)
		rsp, err := svc.Callback(d.Body)
		if err != nil {
			log.Print("RPCService callback error: ", err)
		}
		err = ch.Publish(
			"",
			d.ReplyTo,
			false,
			false,
			amqp.Publishing{
				ContentType:   "application/octet-stream",
				CorrelationId: d.ReplyTo,
				Timestamp:     time.Now(),
				Body:          rsp,
			})
		if err != nil {
			log.Print("failed to publish response for ", d.ReplyTo)
		}
	}
}

func StartRPCServer(endpoint string) {
	for k, v := range gRegisteredRPC {
		go startRPC(endpoint, k, v)
	}
}
