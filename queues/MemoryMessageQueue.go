package queues

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

/*
MemoryMessageQueue Message queue that sends and receives messages within the same process by using shared memory.
This queue is typically used for testing to mock real queues.
Configuration parameters:

  - name:                        name of the message queue

References:

- *:logger:*:*:1.0           (optional)  ILogger components to pass log messages
- *:counters:*:*:1.0         (optional)  ICounters components to pass collected measurements

See MessageQueue
See MessagingCapabilities

Example:

    queue := NewMessageQueue("myqueue");
    queue.Send("123", NewMessageEnvelop("", "mymessage", "ABC"));
	message, err := queue.Receive("123")
        if (message != nil) {
           ...
           queue.Complete("123", message);
        }

*/
type MemoryMessageQueue struct {
	MessageQueue
	messages          []MessageEnvelope
	lockTokenSequence int
	lockedMessages    map[int]*LockedMessage
	opened            bool
	cancel            int32
}

// NewMemoryMessageQueue method are creates a new instance of the message queue.
//   - name  (optional) a queue name.
// Returns: *MemoryMessageQueue
// See MessagingCapabilities
func NewMemoryMessageQueue(name string) *MemoryMessageQueue {
	c := MemoryMessageQueue{}

	c.MessageQueue = *InheritMessageQueue(
		&c, name, NewMessagingCapabilities(true, true, true, true, true, true, true, false, true),
	)

	c.messages = make([]MessageEnvelope, 0)
	c.lockTokenSequence = 0
	c.lockedMessages = make(map[int]*LockedMessage, 0)
	c.opened = false
	c.cancel = 0

	return &c
}

// IsOpen method are checks if the component is opened.
// Return true if the component has been opened and false otherwise.
func (c *MemoryMessageQueue) IsOpen() bool {
	return c.opened
}

// OpenWithParams method are opens the component with given connection and credential parameters.
//   - correlationId     (optional) transaction id to trace execution through call chain.
//   - connection        connection parameters
//   - credential        credential parameters
// Retruns: error or nil no errors occured.
func (c *MemoryMessageQueue) Open(ctx context.Context, correlationId string) (err error) {
	c.opened = true

	c.Logger.Debug(ctx, correlationId, "Opened queue %s", c.Name())

	return nil
}

// Close method are closes component and frees used resources.
//   - correlationId 	(optional) transaction id to trace execution through call chain.
// Returns: error or nil no errors occured.
func (c *MemoryMessageQueue) Close(ctx context.Context, correlationId string) (err error) {
	c.opened = false
	atomic.StoreInt32(&c.cancel, 1)

	c.Logger.Debug(ctx, correlationId, "Closed queue %s", c.Name())

	return nil
}

// Clear method are clears component state.
//   - correlationId 	(optional) transaction id to trace execution through call chain.
// Returns: error or nil no errors occured.
func (c *MemoryMessageQueue) Clear(ctx context.Context, correlationId string) (err error) {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	c.messages = make([]MessageEnvelope, 0)
	c.lockedMessages = make(map[int]*LockedMessage, 0)
	atomic.StoreInt32(&c.cancel, 0)

	return nil
}

// ReadMessageCount method are reads the current number of messages in the queue to be delivered.
// Returns: number of messages or error.
func (c *MemoryMessageQueue) ReadMessageCount() (count int64, err error) {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	count = (int64)(len(c.messages))
	return count, nil
}

// Send method are sends a message into the queue.
//   - correlationId     (optional) transaction id to trace execution through call chain.
//   - envelope          a message envelop to be sent.
// Returns: error or nil for success.
func (c *MemoryMessageQueue) Send(ctx context.Context, correlationId string, envelope *MessageEnvelope) (err error) {
	envelope.SentTime = time.Now()

	// Add message to the queue
	c.Lock.Lock()
	c.messages = append(c.messages, *envelope)
	c.Lock.Unlock()

	c.Counters.IncrementOne(ctx, "queue."+c.Name()+".sent_messages")
	c.Logger.Debug(ctx, envelope.CorrelationId, "Sent message %s via %s", envelope.String(), c.Name())

	return nil
}

// Peek meethod are peeks a single incoming message from the queue without removing it.
// If there are no messages available in the queue it returns nil.
//   - correlationId     (optional) transaction id to trace execution through call chain.
// Returns: a message or error.
func (c *MemoryMessageQueue) Peek(ctx context.Context, correlationId string) (result *MessageEnvelope, err error) {
	var message *MessageEnvelope

	// Pick a message
	c.Lock.Lock()
	if len(c.messages) > 0 {
		message = &c.messages[0]
	}
	c.Lock.Unlock()

	if message != nil {
		c.Logger.Trace(ctx, message.CorrelationId, "Peeked message %s on %s", message, c.String())
	}

	return message, nil
}

// PeekBatch method are peeks multiple incoming messages from the queue without removing them.
// If there are no messages available in the queue it returns an empty list.
//   - correlationId     (optional) transaction id to trace execution through call chain.
//   - messageCount      a maximum number of messages to peek.
// Returns: a list with messages or error.
func (c *MemoryMessageQueue) PeekBatch(ctx context.Context, correlationId string, messageCount int64) (result []*MessageEnvelope, err error) {
	c.Lock.Lock()
	batchMessages := c.messages
	if messageCount <= (int64)(len(batchMessages)) {
		batchMessages = batchMessages[0:messageCount]
	}
	c.Lock.Unlock()

	messages := []*MessageEnvelope{}
	for _, message := range batchMessages {
		messages = append(messages, &message)
	}

	c.Logger.Trace(ctx, correlationId, "Peeked %d messages on %s", len(messages), c.Name())

	return messages, nil
}

//  Receive method are receives an incoming message and removes it from the queue.
//   - correlationId     (optional) transaction id to trace execution through call chain.
//   - waitTimeout       a timeout in milliseconds to wait for a message to come.
// Returns: a message or error.
func (c *MemoryMessageQueue) Receive(ctx context.Context, correlationId string, waitTimeout time.Duration) (*MessageEnvelope, error) {
	messageReceived := false
	var message *MessageEnvelope
	elapsedTime := time.Duration(0)

	for elapsedTime < waitTimeout && !messageReceived {
		c.Lock.Lock()
		if len(c.messages) == 0 {
			c.Lock.Unlock()
			time.Sleep(time.Duration(100) * time.Millisecond)
			elapsedTime += time.Duration(100)
			continue
		}

		// Get message from the queue
		message = &c.messages[0]
		c.messages = c.messages[1:]

		// Generate and set locked token
		lockedToken := c.lockTokenSequence
		c.lockTokenSequence++
		message.SetReference(lockedToken)

		// Add messages to locked messages list
		now := time.Now().Add(waitTimeout)
		lockedMessage := &LockedMessage{
			ExpirationTime: now,
			Message:        message,
			Timeout:        waitTimeout,
		}
		c.lockedMessages[lockedToken] = lockedMessage

		messageReceived = true
		c.Lock.Unlock()
	}

	if message != nil {
		c.Counters.IncrementOne(ctx, "queue."+c.Name()+".received_messages")
		c.Logger.Debug(ctx, message.CorrelationId, "Received message %s via %s", message, c.Name())
	}

	return message, nil
}

// RenewLock method are renews a lock on a message that makes it invisible from other receivers in the queue.
// This method is usually used to extend the message processing time.
//   - message       a message to extend its lock.
//   - lockTimeout   a locking timeout in milliseconds.
// Returns:  error or nil for success.
func (c *MemoryMessageQueue) RenewLock(ctx context.Context, message *MessageEnvelope, lockTimeout time.Duration) (err error) {
	reference := message.GetReference()
	if reference == nil {
		return nil
	}

	c.Lock.Lock()
	// Get message from locked queue
	lockedToken := reference.(int)
	lockedMessage, ok := c.lockedMessages[lockedToken]
	// If lock is found, extend the lock
	if ok {
		now := time.Now()
		// Todo: Shall we skip if the message already expired?
		if lockedMessage.ExpirationTime.After(now) {
			lockedMessage.ExpirationTime = now.Add(lockedMessage.Timeout)
		}
	}
	c.Lock.Unlock()

	c.Logger.Trace(ctx, message.CorrelationId, "Renewed lock for message %s at %s", message, c.Name())

	return nil
}

// Complete method are permanently removes a message from the queue.
// This method is usually used to remove the message after successful processing.
//   - message   a message to remove.
// Returns: error or nil for success.
func (c *MemoryMessageQueue) Complete(ctx context.Context, message *MessageEnvelope) (err error) {
	reference := message.GetReference()
	if reference == nil {
		return nil
	}

	c.Lock.Lock()
	lockedToken := reference.(int)
	delete(c.lockedMessages, lockedToken)
	message.SetReference(nil)
	c.Lock.Unlock()

	c.Logger.Trace(ctx, message.CorrelationId, "Completed message %s at %s", message, c.Name())

	return nil
}

// Abandon method are returnes message into the queue and makes it available for all subscribers to receive it again.
// This method is usually used to return a message which could not be processed at the moment
// to repeat the attempt. Messages that cause unrecoverable errors shall be removed permanently
// or/and send to dead letter queue.
//   - message   a message to return.
// Returns: error or nil for success.
func (c *MemoryMessageQueue) Abandon(ctx context.Context, message *MessageEnvelope) (err error) {
	reference := message.GetReference()
	if reference == nil {
		return nil
	}

	c.Lock.Lock()
	// Get message from locked queue
	lockedToken := reference.(int)
	lockedMessage, ok := c.lockedMessages[lockedToken]
	if ok {
		// Remove from locked messages
		delete(c.lockedMessages, lockedToken)
		message.SetReference(nil)

		// Skip if it is already expired
		if lockedMessage.ExpirationTime.Before(time.Now()) {
			c.Lock.Unlock()
			return nil
		}
	} else { // Skip if it absent
		c.Lock.Unlock()
		return nil
	}
	c.Lock.Unlock()

	c.Logger.Trace(ctx, message.CorrelationId, "Abandoned message %s at %s", message, c.Name())

	// Add back to message queue
	return c.Send(ctx, message.CorrelationId, message)
}

// MoveToDeadLetter method are permanently removes a message from the queue and sends it to dead letter queue.
//   - message   a message to be removed.
// Returns: error or nil for success.
func (c *MemoryMessageQueue) MoveToDeadLetter(ctx context.Context, message *MessageEnvelope) (err error) {
	reference := message.GetReference()
	if reference == nil {
		return nil
	}

	c.Lock.Lock()
	lockedToken := reference.(int)
	delete(c.lockedMessages, lockedToken)
	message.SetReference(nil)
	c.Lock.Unlock()

	c.Counters.IncrementOne(ctx, "queue."+c.Name()+".dead_messages")
	c.Logger.Trace(ctx, message.CorrelationId, "Moved to dead message %s at %s", message, c.Name())

	return nil
}

// Listen method are listens for incoming messages and blocks the current thread until queue is closed.
//   - correlationId     (optional) transaction id to trace execution through call chain.
//   - receiver          a receiver to receive incoming messages.
// See IMessageReceiver
// See Receive
func (c *MemoryMessageQueue) Listen(ctx context.Context, correlationId string, receiver IMessageReceiver) error {
	c.Logger.Trace(ctx, "", "Started listening messages at %s", c.String())

	// Unset cancellation token
	atomic.StoreInt32(&c.cancel, 0)

	for atomic.LoadInt32(&c.cancel) == 0 {
		message, err := c.Receive(ctx, correlationId, time.Duration(1000)*time.Millisecond)
		if err != nil {
			c.Logger.Error(ctx, correlationId, err, "Failed to receive the message")
		}

		if message != nil && atomic.LoadInt32(&c.cancel) == 0 {
			// Todo: shall we recover after panic here??
			func(message *MessageEnvelope) {
				defer func() {
					if r := recover(); r != nil {
						err := fmt.Sprintf("%v", r)
						c.Logger.Error(ctx, correlationId, nil, "Failed to process the message - "+err)
					}
				}()

				err = receiver.ReceiveMessage(ctx, message, c)
				if err != nil {
					c.Logger.Error(ctx, correlationId, err, "Failed to process the message")
				}
			}(message)
		}
	}

	return nil
}

// EndListen method are ends listening for incoming messages.
// When c method is call listen unblocks the thread and execution continues.
//   - correlationId     (optional) transaction id to trace execution through call chain.
func (c *MemoryMessageQueue) EndListen(ctx context.Context, correlationId string) {
	atomic.StoreInt32(&c.cancel, 1)
}
