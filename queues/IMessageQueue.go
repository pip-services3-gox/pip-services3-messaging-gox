package queues

import (
	"context"
	"time"

	crun "github.com/pip-services3-gox/pip-services3-commons-gox/run"
)

// IMessageQueue Interface for asynchronous message queues.
//
// Not all queues may implement all the methods.
// Attempt to call non-supported method will result in NotImplemented exception.
// To verify if specific method is supported consult with MessagingCapabilities.
//
// See MessageEnvelop
// See MessagingCapabilities
type IMessageQueue interface {
	crun.IOpenable

	// Name are gets the queue name
	// Return the queue name.
	Name() string

	// Capabilities method are gets the queue capabilities
	// Return the queue's capabilities object.
	Capabilities() *MessagingCapabilities

	// ReadMessageCount method are reads the current number of messages in the queue to be delivered.
	// Returns number of messages or error.
	ReadMessageCount() (count int64, err error)

	// Send method are sends a message into the queue.
	//  - ctx       operation context
	//  - correlationId     (optional) transaction id to trace execution through call chain.
	//  - envelope          a message envelop to be sent.
	// Returns: error or nil for success.
	Send(ctx context.Context, correlationId string, envelope *MessageEnvelope) error

	// SendAsObject method are sends an object into the queue.
	// Before sending the object is converted into JSON string and wrapped in a MessageEnvelop.
	//  - ctx       operation context
	//  - correlationId     (optional) transaction id to trace execution through call chain.
	//  - messageType       a message type
	//  - value             an object value to be sent
	// Returns: error or nil for success.
	// See Send
	SendAsObject(ctx context.Context, correlationId string, messageType string, value any) error

	// Peek method are peeks a single incoming message from the queue without removing it.
	// If there are no messages available in the queue it returns nil.
	//  - ctx       operation context
	//  - correlationId     (optional) transaction id to trace execution through call chain.
	// Returns: received message or error.
	Peek(ctx context.Context, correlationId string) (result *MessageEnvelope, err error)

	// PeekBatch method are peeks multiple incoming messages from the queue without removing them.
	// If there are no messages available in the queue it returns an empty list.
	//  - ctx       operation context
	//  - correlationId     (optional) transaction id to trace execution through call chain.
	//  - messageCount      a maximum number of messages to peek.
	// Returns:            list with messages or error.
	PeekBatch(ctx context.Context, correlationId string, messageCount int64) (result []*MessageEnvelope, err error)

	// Receive method are receives an incoming message and removes it from the queue.
	//  - ctx       operation context
	//  - correlationId     (optional) transaction id to trace execution through call chain.
	//  - waitTimeout       a timeout in milliseconds to wait for a message to come.
	// Returns: a message or error.
	Receive(ctx context.Context, correlationId string, waitTimeout time.Duration) (result *MessageEnvelope, err error)

	// RenewLock methodd are renews a lock on a message that makes it invisible from other receivers in the queue.
	// This method is usually used to extend the message processing time.
	//  - ctx       operation context
	//  - message       a message to extend its lock.
	//  - lockTimeout   a locking timeout in milliseconds.
	// Returns:      error or nil for success.
	RenewLock(ctx context.Context, message *MessageEnvelope, lockTimeout time.Duration) error

	// Complete method are permanently removes a message from the queue.
	// This method is usually used to remove the message after successful processing.
	//  - ctx       operation context
	//  - message   a message to remove.
	// Returns: error or nil for success.
	Complete(ctx context.Context, message *MessageEnvelope) error

	// Abandon method are returnes message into the queue and makes it available for all subscribers to receive it again.
	// This method is usually used to return a message which could not be processed at the moment
	// to repeat the attempt. Messages that cause unrecoverable errors shall be removed permanently
	// or/and send to dead letter queue.
	//  - ctx       operation context
	//  - message   a message to return.
	// Retruns: error or nil for success.
	Abandon(ctx context.Context, message *MessageEnvelope) error

	// MoveToDeadLetter method are permanently removes a message from the queue and sends it to dead letter queue.
	//  - ctx       operation context
	//  - message   a message to be removed.
	// Results: error or nil for success.
	MoveToDeadLetter(ctx context.Context, message *MessageEnvelope) error

	// Listen method are listens for incoming messages and blocks the current thread until queue is closed.
	//  - ctx       operation context
	//  - correlationId     (optional) transaction id to trace execution through call chain.
	//  - receiver          a receiver to receive incoming messages.
	// See IMessageReceiver
	// See receive
	Listen(ctx context.Context, correlationId string, receiver IMessageReceiver) error

	// BeginListen method are listens for incoming messages without blocking the current thread.
	//  - ctx       operation context
	//  - correlationId     (optional) transaction id to trace execution through call chain.
	//  - receiver          a receiver to receive incoming messages.
	// See listen
	// See IMessageReceiver
	BeginListen(ctx context.Context, correlationId string, receiver IMessageReceiver)

	// EndListen method are ends listening for incoming messages.
	// When this method is call listen unblocks the thread and execution continues.
	//  - ctx       operation context
	//  - correlationId     (optional) transaction id to trace execution through call chain.
	EndListen(ctx context.Context, correlationId string)
}
