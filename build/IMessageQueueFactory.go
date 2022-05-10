package build

import (
	"github.com/pip-services3-gox/pip-services3-messaging-gox/queues"
)

// Creates message queue componens.
type IMessageQueueFactory interface {
	// Creates a message queue component and assigns its name.
	// Parameters:
	//   - name: a name of the created message queue.
	CreateQueue(name string) queues.IMessageQueue
}
