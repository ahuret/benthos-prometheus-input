package input

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/prometheus/prometheus/storage/remote"
)

var prometheusConfigSpec = service.NewConfigSpec().
	Summary("Creates an input that setup an HTTP server, waiting for remove_write requests and extract metrics from it.").
	Field(service.NewStringField("address").Default("http://127.0.0.1:4242")).
	Field(service.NewStringField("path").Default("/receive"))

func newPrometheusInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	address, err := conf.FieldString("address")
	if err != nil {
		return nil, err
	}
	path, err := conf.FieldString("path")
	if err != nil {
		return nil, err
	}
	return &prometheusInput{
		logger:    mgr.Logger(),
		address:   address,
		path:      path,
		responses: make(chan response),
	}, nil
}

func init() {
	if err := service.RegisterBatchInput(
		"prometheus", prometheusConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return newPrometheusInput(conf, mgr)
		}); err != nil {
		panic(err)
	}
}

type prometheusInput struct {
	logger    *service.Logger
	address   string
	path      string
	responses chan response
}

type response struct {
	messages service.MessageBatch
	ack      chan error
}

func (p *prometheusInput) Connect(ctx context.Context) error {
	p.logger.Infof("Prometheus Input: Ready to receive prometheus messages on: %v\n", p.address)
	go func() {
		http.HandleFunc(p.path, p.handleHTTPRequest())
		if err := http.ListenAndServe(p.address, nil); err != nil {
			log.Fatal(err)
		}
	}()
	p.logger.Info("Prometheus Input: Connection ended up")
	return nil
}

func (p *prometheusInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	p.logger.Debug("Prometheus Input: Waiting for a new batch")
	response := <-p.responses
	p.logger.Debugf("Prometheus Input: New batch received (number of messages: %d)", len(response.messages))
	return response.messages,
		func(ctx context.Context, err error) error {
			response.ack <- err
			return nil
		},
		nil
}

func (p *prometheusInput) Close(ctx context.Context) error {
	p.logger.Info("Prometheus Input: closing input")
	return nil
}

func (p *prometheusInput) handleHTTPRequest() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		p.logger.Debug("Prometheus Input: new request received")
		req, err := remote.DecodeWriteRequest(r.Body)
		if err != nil {
			p.logger.Errorf("Prometheus Input: error while decoding request body: %w", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var mm []*service.Message
		for _, ts := range req.Timeseries {
			labels := make(map[string]string, len(ts.Labels))
			for _, l := range ts.Labels {
				labels[l.Name] = l.Value
			}
			name := labels["__name__"]
			for _, s := range ts.Samples {
				m, err := json.Marshal(map[string]interface{}{
					"timestamp": time.Unix(s.Timestamp/1000, 0).UTC().Format(time.RFC3339),
					"value":     strconv.FormatFloat(s.Value, 'f', -1, 64),
					"name":      name,
					"labels":    labels,
				})
				if err != nil {
					p.logger.Errorf("Prometheus Input: error while marshalling metric: %w", err)
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				mm = append(mm, service.NewMessage(m))
			}
		}
		ack := make(chan error, 1)
		p.responses <- response{messages: service.MessageBatch(mm), ack: ack}
		err = <-ack
		close(ack)
		if err != nil {
			p.logger.Errorf("Prometheus Input: ack returned an error: %w", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		p.logger.Debug("Prometheus Input: request has been handle correctly")
		return
	}
}
