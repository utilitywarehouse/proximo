package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	"github.com/pkg/errors"
	"github.com/utilitywarehouse/go-operational/op"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var gitHash string

type link struct {
	description string
	url         string
}

var appMeta = struct {
	name        string
	description string
	owner       string
	slack       string
	links       []link
}{
	name:        "proximo",
	description: "Interoperable GRPC based publish/subscribe",
	owner:       "energy",
	slack:       "#industryparticipation",
	links: []link{
		{"vcs", "https://github.com/utilitywarehouse/proximo"},
	},
}

func main() {
	app := cli.App(appMeta.name, appMeta.description)

	port := app.Int(cli.IntOpt{
		Name:   "port",
		Value:  6868,
		Desc:   "Port to listen on",
		EnvVar: "PROXIMO_PORT",
	})

	probePort := app.Int(cli.IntOpt{
		Name:   "probe-port",
		Value:  8080,
		Desc:   "Port to listen for healtcheck requests",
		EnvVar: "PROXIMO_PROBE_PORT",
	})
	counters := NewCounters()
	var handler handler
	var closer io.Closer
	app.Command("kafka", "Use kafka backend", func(cmd *cli.Cmd) {
		brokers := *cmd.Strings(cli.StringsOpt{
			Name: "brokers",
			Value: []string{
				"localhost:9092",
			},
			Desc:   "Broker addresses e.g., \"server1:9092,server2:9092\"",
			EnvVar: "PROXIMO_KAFKA_BROKERS",
		})
		cmd.Action = func() {
			handler = &kafkaHandler{
				brokers:  brokers,
				counters: counters,
			}
		}
	})
	app.Command("amqp", "Use AMQP backend", func(cmd *cli.Cmd) {
		address := cmd.String(cli.StringOpt{
			Name:   "address",
			Value:  "amqp://localhost:5672",
			Desc:   "Broker address",
			EnvVar: "PROXIMO_AMQP_ADDRESS",
		})
		cmd.Action = func() {
			handler = &amqpHandler{
				address: *address,
			}
		}
	})
	app.Command("nats-streaming", "Use NATS streaming backend", func(cmd *cli.Cmd) {
		url := cmd.String(cli.StringOpt{
			Name:   "url",
			Value:  "nats://localhost:4222",
			Desc:   "NATS url",
			EnvVar: "PROXIMO_NATS_URL",
		})
		cid := cmd.String(cli.StringOpt{
			Name:   "cid",
			Value:  "test-cluster",
			Desc:   "cluster id",
			EnvVar: "PROXIMO_NATS_CLUSTER_ID",
		})
		cmd.Action = func() {
			nh, err := newNatsStreamingHandler(*url, *cid, counters)
			if err != nil {
				log.Panic(err)
			}
			handler = nh
			closer = nh
		}
	})

	app.Command("mem", "Use in-memory testing backend", func(cmd *cli.Cmd) {
		cmd.Action = func() {
			handler = newMemHandler()
		}
	})

	app.After = func() {
		if err := serve(handler, counters, *port, *probePort); err != nil {
			log.Fatal(err)
		}
		if closer != nil {
			if err := closer.Close(); err != nil {
				log.Printf("failed to close connection: %s", err.Error())
			}
		}
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal("App stopped with error")
	}
}

func serve(handler handler, counters counters, port int, probePort int) error {
	serveStatus(handler, probePort)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		errors.Wrap(err, "failed to listen")
	}
	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 5 * time.Minute,
		}),
	}
	grpcServer := grpc.NewServer(opts...)
	RegisterMessageSourceServer(grpcServer, &server{handler, counters})
	RegisterMessageSinkServer(grpcServer, &server{handler, counters})
	if err := grpcServer.Serve(lis); err != nil {
		return errors.Wrap(err, "failed to serve grpc")
	}
	return nil
}

func serveStatus(handler handler, port int) {
	router := mux.NewRouter()

	probe := router.PathPrefix("/__/").Subrouter()
	probe.Methods(http.MethodGet).Handler(op.NewHandler(getOpStatus(handler)))

	server := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: router,
	}
	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Fatal(errors.Wrap(err, "failed to server status"))
		}
	}()
}
