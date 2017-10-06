package main

import (
	"log"
	"reflect"
	"runtime"

	"github.com/rxwen/go-rpc-rabbitmq"
	"github.com/rxwen/resourcepool"
	"github.com/rxwen/resourcepool/rabbitmqpool"
	"github.com/streadway/amqp"
)

const QueueName = "go-rpc-rabbitmq"

const Server = "amqp://guest:guest@t-centos2.lan:5672/"

//const Server = "amqp://guest:guest@rmd-ubuntu.lan:5672/"
var Pool *resourcepool.ResourcePool

func initPool() {
	var err error
	Pool, err = rabbitmqpool.CreateRabbitmqConnectionPool("guest", "guest", "192.168.2.175:5672", 10, 3)
	if err != nil {
		log.Fatal("failed to create rabbitmq pool")
	}
}

var rpcMethodName string

func rpcMethod(req []byte) ([]byte, error) {
	log.Print("get request ", req)
	return []byte("hello world"), nil
}

func request() {
	cc, err := Pool.Get()
	if err != nil {
		log.Fatal("failed to connect ", err)
	}
	defer Pool.Release(cc)
	c := cc.(*amqp.Connection)
	req := []byte("hello w")
	rsp, err := rpcrmq.RequestWithTimeout(c, rpcMethodName, req, 2)
	log.Print("got a response ", rsp, err)

}

func main() {
	rpcMethodName = runtime.FuncForPC(reflect.ValueOf(rpcMethod).Pointer()).Name()
	initPool()

	rpcrmq.RegisterRPC(rpcMethodName, &rpcrmq.RPCService{
		Callback: rpcMethod,
	})
	rpcrmq.StartRPCServer(Server)
	for i := 0; i < 5; i++ {
		request()
	}
}
