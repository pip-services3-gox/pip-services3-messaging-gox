package queues

import (
	"context"
	"sync"

	cconf "github.com/pip-services3-gox/pip-services3-commons-gox/config"
	cerr "github.com/pip-services3-gox/pip-services3-commons-gox/errors"
	cref "github.com/pip-services3-gox/pip-services3-commons-gox/refer"
	cauth "github.com/pip-services3-gox/pip-services3-components-gox/auth"
	cconn "github.com/pip-services3-gox/pip-services3-components-gox/connect"
	ccount "github.com/pip-services3-gox/pip-services3-components-gox/count"
	clog "github.com/pip-services3-gox/pip-services3-components-gox/log"
)

type IMessageQueueOverrides interface {
	IMessageQueue

	// OpenWithParams method are opens the component with given connection and credential parameters.
	//  - correlationId     (optional) transaction id to trace execution through call chain.
	//  - connections        connection parameters
	//  - credential        credential parameters
	// Returns error or nil no errors occured.
	OpenWithParams(ctx context.Context, correlationId string, connections []*cconn.ConnectionParams, credential *cauth.CredentialParams) error
}

/*
MessageQueue message queue that is used as a basis for specific message queue implementations.

Configuration parameters:

  - name:                        name of the message queue
  - connection(s):
    - discovery_key:             key to retrieve parameters from discovery service
    - protocol:                  connection protocol like http, https, tcp, udp
    - host:                      host name or IP address
    - port:                      port number
    - uri:                       resource URI or connection string with all parameters in it
  - credential(s):
    - store_key:                 key to retrieve parameters from credential store
    - username:                  user name
    - password:                  user password
    - access_id:                 application access id
    - access_key:                application secret key

References:

- *:Logger:*:*:1.0           (optional)  ILogger components to pass log messages
- *:Counters:*:*:1.0         (optional)  ICounters components to pass collected measurements
- *:discovery:*:*:1.0        (optional)  IDiscovery components to discover connection(s)
- *:credential-store:*:*:1.0 (optional)  ICredentialStore componetns to lookup credential(s)
*/
type MessageQueue struct {
	Overrides          IMessageQueueOverrides
	Logger             *clog.CompositeLogger
	Counters           *ccount.CompositeCounters
	ConnectionResolver *cconn.ConnectionResolver
	CredentialResolver *cauth.CredentialResolver
	Lock               sync.Mutex
	name               string
	capabilities       *MessagingCapabilities
}

// NewMessageQueue method are creates a new instance of the message queue.
//   - overrides a message queue overrides
//   - name  (optional) a queue name
//   - capabilities (optional) capabilities of this message queue
func InheritMessageQueue(overrides IMessageQueueOverrides, name string, capabilities *MessagingCapabilities) *MessageQueue {
	c := MessageQueue{
		Overrides:    overrides,
		name:         name,
		capabilities: capabilities,
	}
	c.Logger = clog.NewCompositeLogger()
	c.Counters = ccount.NewCompositeCounters()
	c.ConnectionResolver = cconn.NewEmptyConnectionResolver()
	c.CredentialResolver = cauth.NewEmptyCredentialResolver()

	if c.capabilities == nil {
		NewMessagingCapabilities(false, false, false, false, false, false, false, false, false)
	}

	return &c
}

// Name method are gets the queue name
// Return the queue name.
func (c *MessageQueue) Name() string {
	return c.name
}

// Capabilities method are gets the queue capabilities
// Return the queue's capabilities object.
func (c *MessageQueue) Capabilities() *MessagingCapabilities {
	return c.capabilities
}

// Configure method are configures component by passing configuration parameters.
//   - config    configuration parameters to be set.
func (c *MessageQueue) Configure(ctx context.Context, config *cconf.ConfigParams) {
	c.Logger.Configure(ctx, config)
	c.ConnectionResolver.Configure(ctx, config)
	c.CredentialResolver.Configure(ctx, config)

	c.name = cconf.NameResolver.ResolveWithDefault(config, c.name)
	c.name = config.GetAsStringWithDefault("queue", c.name)
}

// SetReferences mmethod are sets references to dependent components.
//   - ctx 			operation context
//   - references 	references to locate the component dependencies.
func (c *MessageQueue) SetReferences(ctx context.Context, references cref.IReferences) {
	c.Logger.SetReferences(ctx, references)
	c.Counters.SetReferences(ctx, references)
	c.ConnectionResolver.SetReferences(ctx, references)
	c.CredentialResolver.SetReferences(ctx, references)
}

// Open method are opens the component.
//   - ctx 			    operation context
//   - correlationId 	(optional) transaction id to trace execution through call chain.
// Returns: error or null no errors occured.
func (c *MessageQueue) Open(ctx context.Context, correlationId string) error {
	connections, err := c.ConnectionResolver.ResolveAll(correlationId)
	if err != nil {
		return err
	}
	if len(connections) == 0 {
		err = cerr.NewConfigError(correlationId, "NO_CONNECTION", "Connection parameters are not set")
		return err
	}

	credential, err := c.CredentialResolver.Lookup(ctx, correlationId)
	if err != nil {
		return err
	}

	return c.Overrides.OpenWithParams(ctx, correlationId, connections, credential)
}

// OpenWithParams method are opens the component with given connection and credential parameters.
//  - ctx 			    operation context
//  - correlationId     (optional) transaction id to trace execution through call chain.
//  - connections        connection parameters
//  - credential         credential parameters
// Returns error or nil no errors occured.
func (c *MessageQueue) OpenWithParams(ctx context.Context, correlationId string, connections []*cconn.ConnectionParams,
	credential *cauth.CredentialParams) error {
	panic("Not supported")
}

// Checks if message queue has been opened
//   - correlationId     (optional) transaction id to trace execution through call chain.
// Returns: error or null for success.
func (c *MessageQueue) CheckOpen(correlationId string) error {
	if !c.Overrides.IsOpen() {
		err := cerr.NewInvalidStateError(
			correlationId,
			"NOT_OPENED",
			"The queue is not opened",
		)
		return err
	}
	return nil
}

// SendAsObject method are sends an object into the queue.
// Before sending the object is converted into JSON string and wrapped in a MessageEnvelop.
//   - ctx 			     operation context
//   - correlationId     (optional) transaction id to trace execution through call chain.
//   - messageType       a message type
//   - value             an object value to be sent
// Returns: error or nil for success.
// See Send
func (c *MessageQueue) SendAsObject(ctx context.Context, correlationId string, messageType string, message any) (err error) {
	envelope := NewMessageEnvelope(correlationId, messageType, nil)
	envelope.SetMessageAsJson(message)
	return c.Overrides.Send(ctx, correlationId, envelope)
}

// BeginListen method are listens for incoming messages without blocking the current thread.
//   - ctx 			     operation context
//   - correlationId     (optional) transaction id to trace execution through call chain.
//   - receiver          a receiver to receive incoming messages.
// See Listen
// See IMessageReceiver
func (c *MessageQueue) BeginListen(ctx context.Context, correlationId string, receiver IMessageReceiver) {
	go func() {
		err := c.Overrides.Listen(ctx, correlationId, receiver)
		if err != nil {
			c.Logger.Error(ctx, correlationId, err, "Failed to listed the message queue "+c.Name())
		}
	}()
}

// String method are gets a string representation of the object.
// Return a string representation of the object.
func (c *MessageQueue) String() string {
	return "[" + c.Name() + "]"
}
