package queues

import "context"

/*
IMessageReceiver callback interface to receive incoming messages.
Example:

    type MyMessageReceiver struct {
      func (c*MyMessageReceiver) ReceiveMessage(ctx context.Context, envelop MessageEnvelop, queue IMessageQueue) {
          fmt.Println("Received message: " + envelop.GetMessageAsString());
      }
    }

    messageQueue := NewMemoryMessageQueue();
    messageQueue.Listen("123", NewMyMessageReceiver());

	opnErr := messageQueue.Open("123")
	if opnErr == nil{
       messageQueue.Send("123", NewMessageEnvelope("", "mymessage", "ABC")); // Output in console: "Received message: ABC"
    }
*/
type IMessageReceiver interface {

	// ReceiveMessage method are receives incoming message from the queue.
	//   - ctx       operation context
	//   - envelope  an incoming message
	//   - queue     a queue where the message comes from
	// See: MessageEnvelope
	// See: IMessageQueue
	ReceiveMessage(ctx context.Context, envelope *MessageEnvelope, queue IMessageQueue) (err error)
}
