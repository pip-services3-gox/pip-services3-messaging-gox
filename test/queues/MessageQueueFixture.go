package test_queues

import (
	"context"
	"testing"
	"time"

	"github.com/pip-services3-gox/pip-services3-messaging-gox/queues"
	"github.com/stretchr/testify/assert"
)

type MessageQueueFixture struct {
	queue queues.IMessageQueue
}

func NewMessageQueueFixture(queue queues.IMessageQueue) *MessageQueueFixture {
	c := MessageQueueFixture{
		queue: queue,
	}
	return &c
}

func (c *MessageQueueFixture) TestSendReceiveMessage(t *testing.T) {
	envelope1 := queues.NewMessageEnvelope("123", "Test", []byte("Test message"))
	sndErr := c.queue.Send(context.TODO(), "", envelope1)
	assert.Nil(t, sndErr)

	envelope2, rcvErr := c.queue.Receive(context.TODO(), "", 10000*time.Millisecond)
	assert.Nil(t, rcvErr)
	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.MessageType, envelope2.MessageType)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.CorrelationId, envelope2.CorrelationId)
}

func (c *MessageQueueFixture) TestReceiveSendMessage(t *testing.T) {
	envelope1 := queues.NewMessageEnvelope("123", "Test", []byte("Test message"))

	time.AfterFunc(500*time.Millisecond, func() {
		c.queue.Send(context.TODO(), "", envelope1)
	})

	envelope2, rcvErr := c.queue.Receive(context.TODO(), "", 10000*time.Millisecond)
	assert.Nil(t, rcvErr)
	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.MessageType, envelope2.MessageType)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.CorrelationId, envelope2.CorrelationId)
}

func (c *MessageQueueFixture) TestReceiveCompleteMessage(t *testing.T) {
	envelope1 := queues.NewMessageEnvelope("123", "Test", []byte("Test message"))
	sndErr := c.queue.Send(context.TODO(), "", envelope1)
	assert.Nil(t, sndErr)

	count, rdErr := c.queue.ReadMessageCount()
	assert.Nil(t, rdErr)
	assert.Greater(t, count, (int64)(0))

	envelope2, rcvErr := c.queue.Receive(context.TODO(), "", 10000*time.Millisecond)
	assert.Nil(t, rcvErr)
	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.MessageType, envelope2.MessageType)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.CorrelationId, envelope2.CorrelationId)

	cplErr := c.queue.Complete(context.TODO(), envelope2)
	assert.Nil(t, cplErr)
	assert.Nil(t, envelope2.GetReference())
}

func (c *MessageQueueFixture) TestReceiveAbandonMessage(t *testing.T) {
	envelope1 := queues.NewMessageEnvelope("123", "Test", []byte("Test message"))
	sndErr := c.queue.Send(context.TODO(), "", envelope1)
	assert.Nil(t, sndErr)

	envelope2, rcvErr := c.queue.Receive(context.TODO(), "", 10000*time.Millisecond)
	assert.Nil(t, rcvErr)
	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.MessageType, envelope2.MessageType)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.CorrelationId, envelope2.CorrelationId)

	abdErr := c.queue.Abandon(context.TODO(), envelope2)
	assert.Nil(t, abdErr)

	envelope2, rcvErr = c.queue.Receive(context.TODO(), "", 10000*time.Millisecond)
	assert.Nil(t, rcvErr)
	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.MessageType, envelope2.MessageType)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.CorrelationId, envelope2.CorrelationId)
}

func (c *MessageQueueFixture) TestSendPeekMessage(t *testing.T) {
	envelope1 := queues.NewMessageEnvelope("123", "Test", []byte("Test message"))
	sndErr := c.queue.Send(context.TODO(), "", envelope1)
	assert.Nil(t, sndErr)

	envelope2, pkErr := c.queue.Peek(context.TODO(), "")
	assert.Nil(t, pkErr)
	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.MessageType, envelope2.MessageType)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.CorrelationId, envelope2.CorrelationId)

	// pop message from queue for next test
	_, rcvErr := c.queue.Receive(context.TODO(), "", 10000*time.Millisecond)
	assert.Nil(t, rcvErr)
}

func (c *MessageQueueFixture) TestPeekNoMessage(t *testing.T) {
	envelope, pkErr := c.queue.Peek(context.TODO(), "")
	assert.Nil(t, pkErr)
	assert.Nil(t, envelope)
}

func (c *MessageQueueFixture) TestMoveToDeadMessage(t *testing.T) {
	envelope1 := queues.NewMessageEnvelope("123", "Test", []byte("Test message"))
	sndErr := c.queue.Send(context.TODO(), "", envelope1)
	assert.Nil(t, sndErr)

	envelope2, rcvErr := c.queue.Receive(context.TODO(), "", 10000*time.Millisecond)
	assert.Nil(t, rcvErr)
	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.MessageType, envelope2.MessageType)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.CorrelationId, envelope2.CorrelationId)

	mvErr := c.queue.MoveToDeadLetter(context.TODO(), envelope2)
	assert.Nil(t, mvErr)
}

func (c *MessageQueueFixture) TestOnMessage(t *testing.T) {
	envelope1 := queues.NewMessageEnvelope("123", "Test", []byte("Test message"))
	receiver := TestMsgReceiver{}
	c.queue.BeginListen(context.TODO(), "", &receiver)

	time.Sleep(1000 * time.Millisecond)

	sndErr := c.queue.Send(context.TODO(), "", envelope1)
	assert.Nil(t, sndErr)

	time.Sleep(1000 * time.Millisecond)

	envelope2 := receiver.Message
	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.MessageType, envelope2.MessageType)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.CorrelationId, envelope2.CorrelationId)

	c.queue.EndListen(context.TODO(), "")
}

type TestMsgReceiver struct {
	Message *queues.MessageEnvelope
}

func (c *TestMsgReceiver) ReceiveMessage(ctx context.Context, message *queues.MessageEnvelope, queue queues.IMessageQueue) (err error) {
	c.Message = message
	return nil
}
