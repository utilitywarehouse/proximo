package main

import (
	"context"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	proximoc "github.com/uw-labs/proximo/proximoc-go"

	stand "github.com/nats-io/nats-streaming-server/server"
	stores "github.com/nats-io/nats-streaming-server/stores"
)

func TestCounterNatsStreaming(t *testing.T) {
	opts := stand.GetDefaultOptions()
	opts.StoreType = stores.TypeMemory

	s, err := stand.RunServerWithOpts(opts, nil)
	defer s.Shutdown()

	counters := NewCounters()
	cr := &CommandReceiver{
		counters: counters,
	}

	topic := "test"
	nh, err := newNatsStreamingHandler("nats://localhost:4222", "test-cluster", counters)
	if err != nil {
		t.Error(err)
	}
	cr.handler = nh
	defer nh.Close()

	go func() {
		cr.Serve("tcp", 6868)
	}()

	endpoint := "127.0.0.1:6868"
	producerContext := context.WithValue(context.Background(), "prox_test", "producer")

	c, err := proximoc.DialProducer(
		producerContext,
		endpoint,
		topic,
	)
	if err != nil {
		t.Error(err)
	}

	sinkCounter := nh.counters.SinkMessagesCounter.WithLabelValues(topic)
	metric := &dto.Metric{}
	sinkCounter.Write(metric)
	if *metric.Counter.Value != 0.0 {
		t.Error("Sink counter should start at 0")
	}

	err = c.Produce([]byte("Hello World"))
	if err != nil {
		t.Error(err)
	}

	metric2 := &dto.Metric{}
	sinkCounter.Write(metric2)
	if *metric2.Counter.Value != 1.0 {
		t.Errorf("Sink counter should start at 1 ... %f", *metric2.Counter.Value)
	}

	err = c.Close()
	if err != nil {
		t.Error(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	consumeContext := context.WithValue(context.Background(), "test", "consume")
	go func() {
		proximoc.ConsumeContext(
			consumeContext,
			endpoint,
			"123",
			topic,
			func(m *proximoc.Message) error {
				wg.Done()
				return nil
			})
	}()

	wg.Wait()
	// We need to give the server a second to write the couter.
	// Is there a place to hook so we can write this better.
	time.Sleep(time.Second * 1)

	sourceCounter := nh.counters.SourcedMessagesCounter.WithLabelValues(topic)
	metric3 := &dto.Metric{}
	sourceCounter.Write(metric3)
	if *metric3.Counter.Value != 1.0 {
		t.Errorf("Source counter should be at 1 ... %f", *metric3.Counter.Value)
	}
}
