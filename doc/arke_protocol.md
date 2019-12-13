# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [api/protobuf-spec/arke.proto](#api/protobuf-spec/arke.proto)
    - [AckResponse](#arke.AckResponse)
    - [Address](#arke.Address)
    - [ConnectResponse](#arke.ConnectResponse)
    - [ConnectionConfiguration](#arke.ConnectionConfiguration)
    - [Consume](#arke.Consume)
    - [ConsumeResponse](#arke.ConsumeResponse)
    - [Credentials](#arke.Credentials)
    - [Empty](#arke.Empty)
    - [Error](#arke.Error)
    - [Filter](#arke.Filter)
    - [Match](#arke.Match)
    - [Message](#arke.Message)
    - [Message.HeadersEntry](#arke.Message.HeadersEntry)
    - [MessageConsumed](#arke.MessageConsumed)
    - [MessageConsumedResponse](#arke.MessageConsumedResponse)
    - [MessageResponse](#arke.MessageResponse)
    - [NackResponse](#arke.NackResponse)
    - [Source](#arke.Source)
    - [Source.OptionsEntry](#arke.Source.OptionsEntry)
  
    - [Address.TargetType](#arke.Address.TargetType)
    - [Filter.MatchType](#arke.Filter.MatchType)
  
  
    - [Consumer](#arke.Consumer)
    - [Producer](#arke.Producer)
  

- [Scalar Value Types](#scalar-value-types)



<a name="api/protobuf-spec/arke.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## api/protobuf-spec/arke.proto
Arke message broker proxy messages.

This file outlines the gRPC interface for the Arke proxy.


<a name="arke.AckResponse"></a>

### AckResponse
Represents the response from AckMessage.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| success | [bool](#bool) |  | Indicates whether the Ack was successful. |
| error | [Error](#arke.Error) |  | Error if the Ack failed. |






<a name="arke.Address"></a>

### Address
Represents the publishing destination for a message.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | The name of this destination address. |
| subjects | [string](#string) | repeated | The subjects of the address. Multiple subjects are allowed on Subscribe, but not on Publish. |
| type | [Address.TargetType](#arke.Address.TargetType) |  | Target type, default is TOPIC. |
| durable | [bool](#bool) |  | Should the address be durable. |
| auto_delete | [bool](#bool) |  | Should the address automatically delete. |
| parent_address | [Address](#arke.Address) |  | A parent Address. Usage includes Address to Address binding. |






<a name="arke.ConnectResponse"></a>

### ConnectResponse
Represents the connection response


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| success | [bool](#bool) |  | Indicates whether the connection was successful. |
| error | [Error](#arke.Error) |  | Error if the connection failed. |






<a name="arke.ConnectionConfiguration"></a>

### ConnectionConfiguration
Represents the broker connection information. This is passed
in by the client allowing us to remain a proxy, but also
allowing us to support multiple providers at once such as
RabbitMQ and Kafka.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| host | [string](#string) |  | Broker hostname or IP address. |
| port | [int32](#int32) |  | Broker port. |
| provider | [string](#string) |  | Provider type, currently only ampq091. |
| tenant | [string](#string) |  | Tenant name for this connection. Tenant is not required |
| credentials | [Credentials](#arke.Credentials) |  | Authentication credentials. |
| ca_certificate | [bytes](#bytes) |  | TLS Certificate authority for broker. Implies tls. |
| tls | [bool](#bool) |  | Should this provider connection use TLS. If used in conjunction with CaCertificate, the certificate will be used for verification. If no CaCertificate is provided then the providers certificate must be trusted by the system certificates. |






<a name="arke.Consume"></a>

### Consume
Sent on the Consume stream from the client to either start consuming messages or to ack/nack messages


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| src | [Source](#arke.Source) |  |  |
| ack | [MessageConsumed](#arke.MessageConsumed) |  |  |






<a name="arke.ConsumeResponse"></a>

### ConsumeResponse
Response to a Consume message. Either a Message or a MessageConsumedResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| msg | [Message](#arke.Message) |  |  |
| consumed_response | [MessageConsumedResponse](#arke.MessageConsumedResponse) |  |  |






<a name="arke.Credentials"></a>

### Credentials
Represents the broker authentication information.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| username | [string](#string) |  | Username for authenticating to broker. |
| password | [string](#string) |  | Password for authenticating to broker. |






<a name="arke.Empty"></a>

### Empty
No parameters or return value.






<a name="arke.Error"></a>

### Error
Represents a generic error message.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  | The text error message. |
| code | [int32](#int32) |  | The error code. |
| is_fatal | [bool](#bool) |  | Indicator that a fatal error has occured, and a reconnect is required on this connection. |






<a name="arke.Filter"></a>

### Filter
Represents a filter for Message.headers. The Filter will prevent a consumer
from consuming all messages from a Source.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| matches | [Match](#arke.Match) | repeated | One or more filter Matches. |
| type | [Filter.MatchType](#arke.Filter.MatchType) |  | The MatchType for this filter. Default is ALL. |






<a name="arke.Match"></a>

### Match
Represents the key/value match for Message.headers. Currently only an
exact match is supported.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | The Message.headers key |
| value | [string](#string) |  | The Message.headers value |






<a name="arke.Message"></a>

### Message
Represents a message that is being produced or consumed. The
error property is set by the proxy when an error occurs during
the consumer process.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| uuid | [string](#string) |  | The proxy sets this UUID when the message is consumed. It should be returned when a message is Ack/Nack&#39;ed. |
| headers | [Message.HeadersEntry](#arke.Message.HeadersEntry) | repeated | The message headers. |
| body | [bytes](#bytes) |  | The message body. |
| address | [Address](#arke.Address) |  | The distination for a published message. |
| persistent | [bool](#bool) |  | Indicates whether to persist the message. |
| error | [Error](#arke.Error) |  | Error message if consuming failed. |






<a name="arke.Message.HeadersEntry"></a>

### Message.HeadersEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="arke.MessageConsumed"></a>

### MessageConsumed
Message used to ack or nack a message UUID


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| uuid | [string](#string) |  | arke.Message UUID returned from Consume. |
| nack | [bool](#bool) |  | Nack (true) or Ack (false) the message. By default all messages will be Ack&#39;d unless you set this to true. |






<a name="arke.MessageConsumedResponse"></a>

### MessageConsumedResponse
Response to a MessageConsumed message


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| uuid | [string](#string) |  | Message UUID of the arke.Message |
| success | [bool](#bool) |  | Was the ack or nack successful. |
| error | [Error](#arke.Error) |  | Error if the MessageConsumed failed |






<a name="arke.MessageResponse"></a>

### MessageResponse
Represents the response from publishing a message.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| success | [bool](#bool) |  | Indicates whether the message was published successfully. |
| error | [Error](#arke.Error) |  | Error if publishing failed. |






<a name="arke.NackResponse"></a>

### NackResponse
Represents the response from NackMessage.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| success | [bool](#bool) |  | Indicates whether the Nack was successful. |
| error | [Error](#arke.Error) |  | Error if the Nack failed. |






<a name="arke.Source"></a>

### Source
Represents the source for consumer subscriptions.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | The name of this source. |
| address | [Address](#arke.Address) |  | The Address associated with this source. |
| durable | [bool](#bool) |  | Should this Source be durable. |
| auto_delete | [bool](#bool) |  | Should this Source automatically delete. |
| options | [Source.OptionsEntry](#arke.Source.OptionsEntry) | repeated | Additional options for this Source. Option keys include: MessageTTL, Expires, DeadLetterAddress, DeadLetterSubject. |
| exclusive | [bool](#bool) |  | Should this source be exclusive to the subscriber. |
| prefetch_count | [int32](#int32) |  | Set the prefetch count for this subscriber. Must be greater than 0. Defaults to 1. |
| filters | [Filter](#arke.Filter) | repeated | Filters for this Source. |






<a name="arke.Source.OptionsEntry"></a>

### Source.OptionsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |





 


<a name="arke.Address.TargetType"></a>

### Address.TargetType


| Name | Number | Description |
| ---- | ------ | ----------- |
| TOPIC | 0 | The address is a message topic. |
| QUEUE | 1 | The address is a message queue. |
| FILTER | 2 | The address is a filtered queue. |



<a name="arke.Filter.MatchType"></a>

### Filter.MatchType


| Name | Number | Description |
| ---- | ------ | ----------- |
| ALL | 0 | All matches must match for a successful filter. |
| ANY | 1 | Any match can match for a successful filter. |


 

 


<a name="arke.Consumer"></a>

### Consumer
Service for consuming messages

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Connect | [ConnectionConfiguration](#arke.ConnectionConfiguration) | [ConnectResponse](#arke.ConnectResponse) | Connect to a message broker. Pass in a ConnectionConfiguration with broker specific connection information. |
| Subscribe | [Source](#arke.Source) | [Message](#arke.Message) stream | Subscribe to a message broker source and receive a stream of messages when they are available. |
| Consume | [Consume](#arke.Consume) stream | [ConsumeResponse](#arke.ConsumeResponse) stream |  |
| AckMessage | [Message](#arke.Message) | [AckResponse](#arke.AckResponse) | Ack a received message. |
| NackMessage | [Message](#arke.Message) | [NackResponse](#arke.NackResponse) | Nack a received message. |
| Disconnect | [Empty](#arke.Empty) | [Empty](#arke.Empty) | Disconnect from the proxy and the message broker. |


<a name="arke.Producer"></a>

### Producer
Service for producing messages

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Connect | [ConnectionConfiguration](#arke.ConnectionConfiguration) | [ConnectResponse](#arke.ConnectResponse) | Connect to a message broker. Pass in a ConnectionConfiguration with broker specific connection information. |
| Publish | [Message](#arke.Message) stream | [MessageResponse](#arke.MessageResponse) stream | Send messages to the message broker. |
| Disconnect | [Empty](#arke.Empty) | [Empty](#arke.Empty) | Disconnect from the proxy and the message broker. |

 



## Scalar Value Types

| .proto Type | Notes | C++ Type | Java Type | Python Type |
| ----------- | ----- | -------- | --------- | ----------- |
| <a name="double" /> double |  | double | double | float |
| <a name="float" /> float |  | float | float | float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long |
| <a name="bool" /> bool |  | bool | boolean | boolean |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str |

