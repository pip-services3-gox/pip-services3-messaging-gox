package queues

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"

	cconv "github.com/pip-services3-gox/pip-services3-commons-gox/convert"
	cdata "github.com/pip-services3-gox/pip-services3-commons-gox/data"
)

/*
MessageEnvelope allows adding additional information to messages. A correlation id, message id, and a message type
are added to the data being sent/received. Additionally, a MessageEnvelope can reference a lock token.
Side note: a MessageEnvelope"s message is stored as a buffer, so strings are converted
using utf8 conversions.
*/
type MessageEnvelope struct {
	reference     interface{}
	CorrelationId string    `json:"correlation_id"` //The unique business transaction id that is used to trace calls across components.
	MessageId     string    `json:"message_id"`     // The message"s auto-generated ID.
	MessageType   string    `json:"message_type"`   // String value that defines the stored message"s type.
	SentTime      time.Time `json:"sent_time"`      // The time at which the message was sent.
	Message       []byte    `json:"message"`        //The stored message.
}

// NewMessageEnvelope method are creates an empty MessageEnvelope
// Returns: *MessageEnvelope new instance
func NewEmptyMessageEnvelope() *MessageEnvelope {
	c := MessageEnvelope{}
	return &c
}

// NewMessageEnvelope method are creates a new MessageEnvelope, which adds a correlation id, message id, and a type to the
// data being sent/received.
//   - correlationId     (optional) transaction id to trace execution through call chain.
//   - messageType       a string value that defines the message"s type.
//   - message           the data being sent/received.
// Returns: *MessageEnvelope new instance
func NewMessageEnvelope(correlationId string, messageType string, message []byte) *MessageEnvelope {
	c := MessageEnvelope{}
	c.CorrelationId = correlationId
	c.MessageType = messageType
	c.MessageId = cdata.IdGenerator.NextLong()
	c.Message = message
	return &c
}

// GetReference method are returns the lock token that this MessageEnvelope references.
func (c *MessageEnvelope) GetReference() interface{} {
	return c.reference
}

// SetReference method are sets a lock token reference for this MessageEnvelope.
//   - value     the lock token to reference.
func (c *MessageEnvelope) SetReference(value interface{}) {
	c.reference = value
}

// GetMessageAsString method are returns the information stored in this message as a string.
func (c *MessageEnvelope) GetMessageAsString() string {
	return string(c.Message)
}

// SetMessageAsString method are stores the given string.
//   - value    the string to set. Will be converted to a bufferg.
func (c *MessageEnvelope) SetMessageAsString(value string) {
	c.Message = []byte(value)
}

// GetMessageAsJson method are returns the value that was stored in this message as a JSON string.
// See  SetMessageAsJson
func (c *MessageEnvelope) GetMessageAsJson() interface{} {
	var result interface{}
	return c.GetMessageAs(result)
}

// SetMessageAsJson method are stores the given value as a JSON string.
//   - value     the value to convert to JSON and store in this message.
// See  GetMessageAsJson
func (c *MessageEnvelope) SetMessageAsJson(value interface{}) {
	c.SetMessageAsObject(value)
}

// GetMessageAs method are returns the value that was stored in this message as object.
// See  SetMessageAsObject
func (c *MessageEnvelope) GetMessageAs(value interface{}) interface{} {
	if c.Message == nil {
		return nil
	}

	err := json.Unmarshal(c.Message, &value)
	if err != nil {
		return nil
	}

	return value
}

// SetMessageAsJson method are stores the given value as a JSON string.
//   - value     the value to convert to JSON and store in this message.
// See  GetMessageAs
func (c *MessageEnvelope) SetMessageAsObject(value interface{}) {
	if value == nil {
		c.Message = []byte{}
	} else {
		message, err := json.Marshal(value)
		if err == nil {
			c.Message = message
		}
	}
}

// String method are convert"s this MessageEnvelope to a string, using the following format:
// <correlation_id>,<MessageType>,<message.toString>
// If any of the values are nil, they will be replaced with ---.
// Returns the generated string.
func (c *MessageEnvelope) String() string {
	builder := strings.Builder{}
	builder.WriteString("[")
	if c.CorrelationId == "" {
		builder.WriteString("---")
	} else {
		builder.WriteString(c.CorrelationId)
	}
	builder.WriteString(",")
	if c.MessageType == "" {
		builder.WriteString("---")
	} else {
		builder.WriteString(c.MessageType)
	}
	builder.WriteString(",")
	if c.Message == nil {
		builder.WriteString("---")
	} else {
		builder.Write(c.Message)
	}
	builder.WriteString("]")
	return builder.String()
}

func (c *MessageEnvelope) MarshalJSON() ([]byte, error) {
	jsonData := map[string]interface{}{
		"message_id":     c.MessageId,
		"correlation_id": c.CorrelationId,
		"message_type":   c.MessageType,
	}

	if !c.SentTime.IsZero() {
		jsonData["sent_time"] = c.SentTime
	} else {
		jsonData["sent_time"] = time.Now()
	}

	if c.Message != nil {
		base64Text := make([]byte, base64.StdEncoding.EncodedLen(len(c.Message)))
		base64.StdEncoding.Encode(base64Text, []byte(c.Message))
		jsonData["message"] = string(base64Text)
	}

	return json.Marshal(jsonData)
}

func (c *MessageEnvelope) UnmarshalJSON(data []byte) error {
	var jsonData map[string]interface{}
	err := json.Unmarshal(data, &jsonData)
	if err != nil {
		return err
	}

	c.MessageId = jsonData["message_id"].(string)
	c.CorrelationId = jsonData["correlation_id"].(string)
	c.MessageType = jsonData["message_type"].(string)
	c.SentTime = cconv.DateTimeConverter.ToDateTime(jsonData["sent_time"])

	base64Text, ok := jsonData["message"].(string)
	if ok && base64Text != "" {
		data := make([]byte, base64.StdEncoding.DecodedLen(len(base64Text)))
		len, err := base64.StdEncoding.Decode(data, []byte(base64Text))
		if err != nil {
			return err
		}
		c.Message = data[:len]
	}

	return nil
}
