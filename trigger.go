package flogonsqtrigger

import (
	"context"
	"fmt"

	"github.com/TIBCOSoftware/flogo-lib/core/trigger"
	"github.com/TIBCOSoftware/flogo-lib/logger"
	nsq "github.com/nsqio/go-nsq"
)

var log = logger.GetLogger("trigger-flogo-flogonsqtrigger")

// NsqTrigger is NSQ message trigger
type NsqTrigger struct {
	metadata  *trigger.Metadata
	config    *trigger.Config
	handlers  []*trigger.Handler
	consumers []*nsq.Consumer
}

//NewFactory create a new Trigger factory
func NewFactory(md *trigger.Metadata) trigger.Factory {
	return &NsqFactory{metadata: md}
}

// NsqFactory Trigger factory
type NsqFactory struct {
	metadata *trigger.Metadata
}

//New Creates a new trigger instance for a given id
func (t *NsqFactory) New(config *trigger.Config) trigger.Trigger {
	return &NsqTrigger{metadata: t.metadata, config: config}
}

// Metadata implements trigger.Trigger.Metadata
func (t *NsqTrigger) Metadata() *trigger.Metadata {
	return t.metadata
}

// Initialize implements trigger.Init
func (t *NsqTrigger) Initialize(ctx trigger.InitContext) error {
	log.Info("Initializing NSQ trigger...")
	t.handlers = ctx.GetHandlers()
	return nil
}

// Start implements ext.Trigger.Start
func (t *NsqTrigger) Start() error {

	handlers := t.handlers

	for _, handler := range handlers {

		nsqlds := handler.GetStringSetting("NsqlookupdAddress")
		topic := handler.GetStringSetting("Topic")
		channel := handler.GetStringSetting("Channel")

		config := nsq.NewConfig()
		q, err := nsq.NewConsumer(topic, channel, config)
		t.consumers = append(t.consumers, q)
		q.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
			t.RunHandler(handler, string(message.Body))
			return nil
		}))

		if err != nil {
			log.Info("Could not add NSQ consumer: %s", err)
		}

		// NSQLookupd addresses
		err = q.ConnectToNSQLookupd(nsqlds)
		if err != nil {
			log.Info("Could not connect to NSQ")
		}
	}
	return nil
}

// Stop implements ext.Trigger.Stop
func (t *NsqTrigger) Stop() error {
	fmt.Printf("Stopping NSQ...")
	for _, q := range t.consumers {
		q.Stop()
	}
	return nil
}

// RunHandler action on new NSQ message
func (t *NsqTrigger) RunHandler(handler *trigger.Handler, payload string) {

	trgData := make(map[string]interface{})
	trgData["message"] = payload

	_, err := handler.Handle(context.Background(), trgData)

	if err != nil {
		fmt.Printf("Error starting action: ", err.Error())
	}

	//fmt.Printf("Ran Handler: [%s]", handler)

}
