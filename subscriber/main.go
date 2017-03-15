package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/naveego/api/pipeline/subscriber"
	"github.com/naveego/navigator-go/subscribers/protocol"
	"github.com/naveego/navigator-go/subscribers/server"
	"github.com/naveego/pipeline-subscribers/sql/oracle"
	_ "gopkg.in/rana/ora.v4"
)

var (
	verbose = flag.Bool("v", false, "enable verbose logging")
)

func main() {

	logrus.SetOutput(os.Stdout)

	if len(os.Args) < 2 {
		fmt.Println("Not enough arguments.")
		os.Exit(-1)
	}

	flag.Parse()

	addr := os.Args[1]

	if *verbose {
		logrus.SetLevel(logrus.DebugLevel)
	}

	srv := server.NewSubscriberServer(addr, &subscriberHandler{})

	err := srv.ListenAndServe()
	if err != nil {
		logrus.Fatal("Error shutting down server: ", err)
	}
}

type subscriberHandler struct{}

func (h *subscriberHandler) TestConnection(request protocol.TestConnectionRequest) (protocol.TestConnectionResponse, error) {
	sub := oracle.NewSubscriber()
	ctx := subscriber.Context{}

	success, msg, err := sub.TestConnection(ctx, request.Settings)
	if err != nil {
		return protocol.TestConnectionResponse{}, err
	}

	return protocol.TestConnectionResponse{
		Success: success,
		Message: msg,
	}, nil
}

func (h *subscriberHandler) DiscoverShapes(request protocol.DiscoverShapesRequest) (protocol.DiscoverShapesResponse, error) {
	pub := oracle.NewSubscriber()
	ctx := subscriber.Context{
		Subscriber: request.SubscriberInstance,
	}

	shapes, err := pub.Shapes(ctx)
	if err != nil {
		return protocol.DiscoverShapesResponse{}, err
	}

	return protocol.DiscoverShapesResponse{
		Shapes: shapes,
	}, nil
}

func (h *subscriberHandler) ReceiveDataPoint(request protocol.ReceiveShapeRequest) (protocol.ReceiveShapeResponse, error) {
	sub := oracle.NewSubscriber()
	ctx := subscriber.Context{
		Subscriber: request.SubscriberInstance,
		Pipeline:   request.Pipeline,
	}

	err := sub.Receive(ctx, request.Shape, request.DataPoint)
	if err != nil {
		return protocol.ReceiveShapeResponse{Success: false, Message: err.Error()}, nil
	}

	return protocol.ReceiveShapeResponse{
		Success: true,
	}, nil
}
