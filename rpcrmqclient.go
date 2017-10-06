package rpcrmq

import (
	"errors"
	"fmt"
	"log"
	"time"

	uuid "github.com/nu7hatch/gouuid"
	"github.com/streadway/amqp"
)

var gRequestTimeoutSecond, gReryCount int

func Init(endpoint string, requestTimeoutS, retryCount int) {
	gRequestTimeoutSecond = requestTimeoutS
	gReryCount = retryCount
}

func RequestWithTimeout(con *amqp.Connection, method string, request []byte, timeoutS int) ([]byte, error) {
	ch, err := con.Channel()
	if err != nil {
		log.Print("failed to open a channel", err)
		return nil, err
	}
	defer ch.Close()

	id, _ := uuid.NewV4()

	//args := make(map[string]interface{})
	args := amqp.Table{}
	// TODO: the x-message-ttl doesn't work
	//if timeoutS > 0 {
	//args["x-message-ttl"] = int32(timeoutS * 1000)
	////args = amqp.Table{"x-message-ttl": int32(timeoutS * 1000)}
	//log.Print("timeoutS is ", args["x-message-ttl"])
	//} else if gRequestTimeoutSecond > 0 {
	//args["x-message-ttl"] = int32(gRequestTimeoutSecond * 1000)
	////args = amqp.Table{"x-message-ttl": int32(gRequestTimeoutSecond * 1000)}
	//log.Print("timeoutS is ", args["x-message-ttl"])
	//}
	q, err := ensureServerQueue(ch, method)
	if err != nil {
		log.Print("failed to declare request queue to make sure it exists", err)
		return nil, err
	}
	q, err = ch.QueueDeclare(
		method+"."+id.String(), //name
		false, // durable
		true,  // autoDelete
		false, // exclusive
		false, //noWait
		args,  // args
	)
	if err != nil {
		log.Print("failed to declare response queue ", err)
		return nil, err
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // autoAck
		true,   // exclusive
		false,  // noLocal
		false,  // noWait
		nil,    // args
	)
	pub := amqp.Publishing{
		ContentType:   "application/octet-stream",
		CorrelationId: q.Name,
		Timestamp:     time.Now(),
		ReplyTo:       q.Name,
		Body:          request,
	}
	if timeoutS > 0 {
		pub.Expiration = fmt.Sprintf("%d", timeoutS*1000)
	} else if gRequestTimeoutSecond > 0 {
		pub.Expiration = fmt.Sprintf("%d", gRequestTimeoutSecond*1000)
	}

	err = ch.Publish(
		"",
		method,
		false,
		false,
		pub)
	if err != nil {
		log.Print("failed to publish request ", q.Name)
		return nil, err
	}
	log.Print("request published to ", method)

	//closeErrors := make(chan *amqp.Error)
	//c.NotifyClose(closeErrors)
	//e := <-closeErrors
	to := time.Duration(999999999) * time.Second
	if timeoutS > 0 {
		to = time.Duration(timeoutS) * time.Second
	} else if gRequestTimeoutSecond > 0 {
		to = time.Duration(gRequestTimeoutSecond) * time.Second
	}

	for {
		select {
		case d := <-msgs:
			log.Print("got response ", d)
			if d.CorrelationId != q.Name {
				return nil, errors.New("invalid correction id on response queue, " + d.CorrelationId + " vs " + q.Name)
			}
			return d.Body, nil
		case <-time.After(to):
			return nil, errors.New("wait response timeout for " + q.Name)
		}
	}
}

func Request(con *amqp.Connection, method string, request []byte) ([]byte, error) {
	return RequestWithTimeout(con, method, request, 0)
}
