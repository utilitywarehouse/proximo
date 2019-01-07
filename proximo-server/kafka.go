package main

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/pkg/errors"
	"google.golang.org/grpc/grpclog"
)

type kafkaHandler struct {
	brokers  []string
	version  *sarama.KafkaVersion
	counters counters
}

func (h *kafkaHandler) HandleConsume(ctx context.Context, conf consumerConfig, forClient chan<- *Message, confirmRequest <-chan *Confirmation) error {
	toConfirmIds := make(chan string)

	errors := make(chan error)

	// TODO: un hardcode some of this stuff
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Metadata.RefreshFrequency = 30 * time.Second
	if h.version != nil {
		config.Version = *h.version
	}

	c, err := cluster.NewConsumer(h.brokers, conf.consumer, []string{conf.topic}, config)
	if err != nil {
		return err
	}

	defer func() {
		_ = c.Close()
	}()

	go func() {
		err := h.consume(ctx, c, forClient, toConfirmIds, conf.topic, conf.consumer)
		if err != nil {
			errors <- err
		}
	}()

	var toConfirm []string
	for {
		select {
		case tc := <-toConfirmIds:
			toConfirm = append(toConfirm, tc)
		case cr := <-confirmRequest:
			if len(toConfirm) < 1 {
				return errInvalidConfirm
			}
			if toConfirm[0] != cr.GetMsgID() {
				return errInvalidConfirm
			}
			err := h.confirm(ctx, c, cr.GetMsgID(), conf.topic)
			if err != nil {
				return err
			}
			h.counters.SourcedMessagesCounter.WithLabelValues(conf.topic).Inc()
			toConfirm = toConfirm[1:]
		case <-ctx.Done():
			return nil
		case err := <-errors:
			return err
		}
	}
}

func (h *kafkaHandler) consume(ctx context.Context, c *cluster.Consumer, forClient chan<- *Message, toConfirmID chan string, topic, consumer string) error {

	grpclog.Println("started consume loop")
	defer grpclog.Println("exited consume loop")

	for {
		select {
		case msg, ok := <-c.Messages():
			if !ok {
				return errors.New("kafka message channel was closed")
			}
			confirmID := fmt.Sprintf("%d-%d", msg.Offset, msg.Partition)
			select {
			case toConfirmID <- confirmID:
			case <-ctx.Done():
				grpclog.Println("context is done")
				return c.Close()
			}
			select {
			case forClient <- &Message{Data: msg.Value, Id: confirmID}:
			case <-ctx.Done():
				grpclog.Println("context is done")
				return c.Close()
			}
		case err := <-c.Errors():
			grpclog.Printf("kafka error causing consume exit %v\n", err)
			return err
		case <-ctx.Done():
			grpclog.Println("context is done")
			return c.Close()
		}
	}
}

func (h *kafkaHandler) confirm(ctx context.Context, c *cluster.Consumer, id string, topic string) error {
	spl := strings.Split(id, "-")
	o, err := strconv.ParseInt(spl[0], 10, 64)
	if err != nil {
		return fmt.Errorf("error parsing message id '%s' : %s", id, err.Error())
	}
	p, err := strconv.ParseInt(spl[1], 10, 32)
	if err != nil {
		return fmt.Errorf("error parsing message id '%s' : %s", id, err.Error())
	}
	c.MarkPartitionOffset(topic, int32(p), o, "")
	return nil
}

func (h *kafkaHandler) HandleProduce(ctx context.Context, cfg producerConfig, forClient chan<- *Confirmation, messages <-chan *Message) error {
	conf := sarama.NewConfig()
	conf.Producer.Return.Successes = true
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Return.Errors = true
	conf.Producer.Retry.Max = 3
	conf.Producer.Timeout = time.Duration(60) * time.Second

	sp, err := sarama.NewSyncProducer(h.brokers, conf)
	if err != nil {
		return err
	}
	defer sp.Close()

	for {
		select {
		case m := <-messages:
			pm := &sarama.ProducerMessage{
				Topic: cfg.topic,
				Value: sarama.ByteEncoder(m.GetData()),
				// Key = TODO:
			}

			_, _, err = sp.SendMessage(pm)
			if err != nil {
				return err
			}
			h.counters.SinkMessagesCounter.WithLabelValues(cfg.topic).Inc()
			forClient <- &Confirmation{MsgID: m.GetId()}
		case <-ctx.Done():
			return nil
		}
	}
}

func (h *kafkaHandler) Status() (bool, error) {
	errs := make(chan error)

	for _, broker := range h.brokers {
		go func(broker string) {
			conn, err := net.DialTimeout("tcp", broker, 10*time.Second)
			if err != nil {
				errs <- fmt.Errorf("Failed to connect to broker %s: %v", broker, err)
				return
			}
			if err = conn.Close(); err != nil {
				errs <- fmt.Errorf("Failed to close connection to broker %s: %v", broker, err)
				return
			}
			errs <- nil
		}(broker)
	}

	e := []error{}
	for range h.brokers {
		err := <-errs
		if err != nil {
			e = append(e, err)
		}
	}
	if len(e) == 0 {
		return true, nil
	}
	return false, errors.Errorf("Series of errors: %v", e)

}
