package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gorilla/mux"
	cli "github.com/jawher/mow.cli"
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

	endpoints := app.String(cli.StringOpt{
		Name:   "endpoints",
		Value:  "consume,publish",
		Desc:   "The proximo endpoints to expose (consume, publish)",
		EnvVar: "PROXIMO_ENDPOINTS",
	})

	app.Command("kafka", "Use kafka backend", func(cmd *cli.Cmd) {
		brokers := *cmd.Strings(cli.StringsOpt{
			Name: "brokers",
			Value: []string{
				"localhost:9092",
			},
			Desc:   "Broker addresses e.g., \"server1:9092,server2:9092\"",
			EnvVar: "PROXIMO_KAFKA_BROKERS",
		})
		kafkaVersion := cmd.String(cli.StringOpt{
			Name:   "version",
			Desc:   "Kafka Version e.g. 1.1.1, 0.10.2.0",
			EnvVar: "PROXIMO_KAFKA_VERSION",
		})

		cmd.Action = func() {
			var version *sarama.KafkaVersion
			if kafkaVersion != nil && *kafkaVersion != "" {
				kv, err := sarama.ParseKafkaVersion(*kafkaVersion)
				if err != nil {
					log.Fatalf("failed to parse kafka version: %v ", err)
				}
				version = &kv
			}
			handler := &kafkaHandler{
				brokers:  brokers,
				counters: counters,
				version:  version,
			}
			log.Printf("Using kafka at %s\n", brokers)
			if err := serve(handler, counters, *port, *probePort, *endpoints); err != nil {
				log.Fatal(err)
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
			handler := &amqpHandler{
				address: *address,
			}
			log.Printf("Using AMQP at %s\n", *address)
			if err := serve(handler, counters, *port, *probePort, *endpoints); err != nil {
				log.Fatal(err)
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
			log.Printf("Using NATS streaming server at %s with cluster id %s\n", *url, *cid)
			if err := serve(nh, counters, *port, *probePort, *endpoints); err != nil {
				log.Fatal(err)
			}
		}
	})

	app.Command("mem", "Use in-memory testing backend", func(cmd *cli.Cmd) {
		cmd.Action = func() {
			handler := newMemHandler()
			log.Printf("Using in memory testing backend")
			if err := serve(handler, counters, *port, *probePort, *endpoints); err != nil {
				log.Fatal(err)
			}
		}
	})

	if err := app.Run(os.Args); err != nil {
		log.Fatal("App stopped with error")
	}
}

func serve(handler handler, counters counters, port int, probePort int, endpoints string) error {
	serveStatus(handler, probePort)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return errors.Wrap(err, "failed to listen")
	}
	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 5 * time.Minute,
		}),
	}
	grpcServer := grpc.NewServer(opts...)
	registerGRPCServers(grpcServer, &server{handler, counters}, endpoints)
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

func registerGRPCServers(grpcServer *grpc.Server, proximoServer *server, endpoints string) {
	for _, endpoint := range strings.Split(endpoints, ",") {
		switch endpoint {
		case "consume":
			RegisterMessageSourceServer(grpcServer, proximoServer)
		case "publish":
			RegisterMessageSinkServer(grpcServer, proximoServer)
		default:
			log.Fatalf("invalid expose-endpoint flag: %s", endpoint)
		}
	}
}
