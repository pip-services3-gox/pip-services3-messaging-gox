# <img src="https://uploads-ssl.webflow.com/5ea5d3315186cf5ec60c3ee4/5edf1c94ce4c859f2b188094_logo.svg" alt="Pip.Services Logo" width="200"> <br/> IoC container for Golang Changelog

## <a name="1.0.0"></a> 1.1.0 (2020-03-23)

Improved message queues

### Features
* **queues** Added CallbackMessageReceiver to wrap callbacks into IMessageReceiver interface
* **queues** Addded IMessageConnection interface
* **build** Set config params and references to created queues in MessageQueueFactory
* **queues** Added CheckOpen method to MessageQueue
* **queues** Added JSON serialization for MessageEnvelop
* **build** Added IMessageQueueFactory interface
* **build** Added MessageQueueFactory abstract class

## <a name="1.0.0"></a> 1.0.0 (2020-03-05)

Initial public release

### Features
* **build** in-memory message queue factory
* **queues** interfaces for working with message queues, subscriptions for receiving messages from the queue, and an in-memory message queue implementation