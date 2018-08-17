package nsqtrigger

import (
	"context"
	"fmt"

	"github.com/TIBCOSoftware/flogo-lib/core/trigger"
	"github.com/TIBCOSoftware/flogo-lib/logger"
	nsq "github.com/nsqio/go-nsq"
)

var log = logger.GetLogger("trigger-flogo-nsqtrigger")

type CreateHandler struct{}

// NsqTrigger is NSQ message trigger
type NsqTrigger struct {
	metadata *trigger.Metadata
	config   *trigger.Config
	handlers []*trigger.Handler
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

	fmt.Printf("Starting NSQ trigger...")
	handlers := t.handlers

	for _, handler := range handlers {

		nsqlds := handler.GetStringSetting("NsqlookupdAddress")
		topic := handler.GetStringSetting("Topic")

		fmt.Println("Starting NSQ Consumer.")

		//createConsumer, err := NewConsumer(topic, topic, &CreateHandler{})
		config := nsq.NewConfig()
		q, err := nsq.NewConsumer(topic, "ch_"+topic, config)
		q.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
			log.Info("Got a message: %v", message)
			t.RunHandler(handler, string(message.Body))
			return nil
		}))

		if err != nil {
			log.Info("Could not add consumer: %s", err)
		}

		// NSQLookupd addresses
		err = q.ConnectToNSQLookupd(nsqlds)
		if err != nil {
			log.Info("Could not connect")
		}
	}
	return nil
}

// Stop implements ext.Trigger.Stop
func (t *NsqTrigger) Stop() error {

	fmt.Printf("Stopping NSQ..")
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

	fmt.Printf("Ran Handler: [%s]", handler)

}
