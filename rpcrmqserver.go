package rpcrmq

import (
	"errors"
	"log"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/streadway/amqp"
)

type RPCService struct {
	Callback func([]byte) ([]byte, error)
}

const MessageTypeRequest = "Request"
const MessageTypeResponse = "Response"
const MessageTypeError = "Error"

var gRegisteredRPC map[string]*RPCService = make(map[string]*RPCService)

func RegisterRPC(rpcName string, svc *RPCService) error {
	if _, exist := gRegisteredRPC[rpcName]; exist {
		return errors.New(rpcName + " service has already been registered")
	}
	gRegisteredRPC[rpcName] = svc
	return nil
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

func rpcInner(endpoint, method string, svc *RPCService) {
	con, err := amqp.Dial(endpoint)
	if err != nil {
		log.Print("failed to connect ", err)
		return
	}
	defer con.Close()

	closeErrors := make(chan *amqp.Error)
	con.NotifyClose(closeErrors)

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
	select {
	case d := <-msgs:
		{
			rsp, err := svc.Callback(d.Body)
			msgType := MessageTypeResponse
			if err != nil {
				log.Print("RPCService callback error: ", err)
				msgType = MessageTypeError
				var errMsg = ErrorMessage{
					Message: err.Error(),
				}
				rsp, err = proto.Marshal(&errMsg)
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
					Type:          msgType,
					Body:          rsp,
				})
			if err != nil {
				log.Print("failed to publish response for ", d.ReplyTo)
			}
		}
	case e := <-closeErrors:
		{
			log.Println("connection closed, restart server, error:", e)
			return
		}
	}
}

func startRPC(endpoint, method string, svc *RPCService) {
	for {
		rpcInner(endpoint, method, svc)
	}
}

func StartRPCServer(endpoint string) {
	for k, v := range gRegisteredRPC {
		go startRPC(endpoint, k, v)
	}
}
