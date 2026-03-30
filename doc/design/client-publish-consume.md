# Client Publish / Consume Guide

## Purpose

This document shows how a client connects to Arke, publishes messages, and
receives and processes messages through the gRPC API.

It is written from the application client's perspective. For the full protobuf
field reference, see [doc/arke_protocol.md](../arke_protocol.md).

---

## What a Client Connects To

Your application connects to Arke over gRPC, not directly to the broker.
Arke then opens and manages the broker connection using the
`ConnectionConfiguration` you send in the `Connect` RPC.

```text
Your application
     │
     │ gRPC
     ▼
   Arke
     │
     │ AMQP 0.9.1 / RabbitMQ Streams
     ▼
Message broker
```

Currently, the only supported provider is `amqp091`.

---

## Client Session Model

Arke keeps broker connection state per gRPC client connection.

- Call `Producer.Connect` before `Publish` or `PublishOne`.
- Call `Consumer.Connect` before `Consume`.
- Keep the connection open for the lifetime of the producer or consumer session.
- Call `Disconnect` when shutting down cleanly.

---

## End-to-End Flow

### Produce

```text
grpc.Dial(arke)
   │
   ├── Producer.Connect(ConnectionConfiguration)
   │
   ├── Producer.PublishOne(Message)
   │     or
   ├── Producer.Publish(stream Message)
   │
   └── Producer.Disconnect()
```

### Consume

```text
grpc.Dial(arke)
   │
   ├── Consumer.Connect(ConnectionConfiguration)
   │
   ├── Consumer.Consume(stream Consume)
   │       │
   │       ├── client sends one Consume{Src: Source}
   │       ├── server streams back ConsumeResponse{Msg: Message}
   │       └── client sends Consume{Ack: MessageConsumed}
   │
   └── Consumer.Disconnect()
```

Only one `Source` declaration is allowed per `Consume` stream. After that first
`Source`, the client keeps the same stream open and sends `Ack` or `Nack`
messages back on that stream as it processes deliveries.

---

## Step 1: Create a gRPC Connection to Arke

Connect to Arke using normal gRPC client setup.

- Use TLS transport credentials if Arke itself is configured with
  `ARKE_CERT_FILE` and `ARKE_CERT_KEY`.
- Use insecure credentials only when Arke is running without TLS.
  instances.

Example:

```go
conn, err := grpc.NewClient(
    "arke.example.internal:50051",
    grpc.WithTransportCredentials(insecure.NewCredentials()),
)
if err != nil {
    return err
}
defer conn.Close()
```

---

## Step 2: Connect Arke to the Broker

After dialing Arke, call `Connect` on the service you will use.

Example `ConnectionConfiguration`:

```go
cfg := &pb.ConnectionConfiguration{
    Host:     "rabbitmq.internal",
    Port:     5672,
    Provider: "amqp091",
    Credentials: &pb.Credentials{
        Username: "appuser",
        Password: "apppassword",
    },
    ClientName: "orders-api-producer",
}
```

Important fields:

| Field            | Required    | Meaning                                                      |
| ---------------- | ----------- | ------------------------------------------------------------ |
| `host`           | yes         | Broker hostname or IP                                        |
| `port`           | yes         | Broker port                                                  |
| `provider`       | yes         | Broker provider type, currently `amqp091`                    |
| `credentials`    | usually     | Broker username/password                                     |
| `tls`            | optional    | Enables TLS on the broker connection                         |
| `ca_certificate` | optional    | PEM CA bytes used to verify the broker certificate           |
| `client_name`    | recommended | Human-meaningful name used in the client identifier          |
| `admin_port`     | optional    | Provider-specific admin port used for some broker operations |

For a producer:

```go
producer := pb.NewProducerClient(conn)

resp, err := producer.Connect(ctx, cfg)
if err != nil {
    return err
}
if !resp.GetSuccess() {
    return fmt.Errorf("producer connect failed: %s", resp.GetError().GetMessage())
}
```

For a consumer, the call is the same but made through `pb.NewConsumerClient`.

### Broker TLS

`ConnectionConfiguration.Tls` controls TLS on the broker side, not on the gRPC
connection to Arke.

- Use gRPC transport credentials to secure the application-to-Arke hop.
- Use `ConnectionConfiguration.Tls` and `CaCertificate` to secure the
  Arke-to-broker hop.

---

## Step 3: Consume, Process, and Ack Messages

It is important that a consumer start consuming before a publisher publishes. The
consumer will automatically create the source if it doesn't exist, but the
publisher will fail if the source doesn't exist.

`Consumer.Consume` is a bidirectional stream with two client message types:

- `Consume{Src: Source}` starts the subscription.
- `Consume{Ack: MessageConsumed}` acknowledges or rejects a delivered message.

### 1. Get the consume stream

```go
consumer := pb.NewConsumerClient(conn)

resp, err := consumer.Connect(ctx, cfg)
if err != nil {
    return err
}
if !resp.GetSuccess() {
    return fmt.Errorf("consumer connect failed: %s", resp.GetError().GetMessage())
}

stream, err := consumer.Consume(ctx)
if err != nil {
    return err
}
```

### 2. Send one `Source`

```go
source := &pb.Source{
    Name: "orders-worker",
    Address: &pb.Address{
        Name:     "orders.exchange",
        Subjects: []string{"orders.created"},
        Type:     pb.Address_TOPIC,
    },
    Type:          pb.Source_QUEUE,
    AutoDelete:    false,
    Exclusive:     false,
    PrefetchCount: 10,
    Options: map[string]string{
        "DeadLetterAddress": "orders.dlx",
        "DeadLetterSubject": "orders.failed",
    },
}

if err := stream.Send(&pb.Consume{
    Msg: &pb.Consume_Src{Src: source},
}); err != nil {
    return err
}
```

After sending the `Source`, do not send another source on the same stream.
If you need to consume from another source, open another `Consume` stream.

### 3. Receive messages and process them

Each delivery arrives as `ConsumeResponse{Msg: Message}`.

Key fields on the delivered `Message`:

| Field     | Meaning                                             |
| --------- | --------------------------------------------------- |
| `uuid`    | Arke-generated message identifier used for Ack/Nack |
| `headers` | Broker/application headers                          |
| `body`    | Message payload                                     |
| `address` | Address associated with the delivery                |
| `error`   | Error information if the delivery itself failed     |

Typical receive loop:

```go
for {
    resp, err := stream.Recv()
    if err == io.EOF {
        return nil
    }
    if err != nil {
        return err
    }

    switch {
    case resp.GetMsg() != nil:
        msg := resp.GetMsg()

        processErr := handleMessage(msg)

        ack := &pb.MessageConsumed{Uuid: msg.GetUuid()}
        if processErr != nil {
            ack.Nack = true
        }

        if err := stream.Send(&pb.Consume{
            Msg: &pb.Consume_Ack{Ack: ack},
        }); err != nil {
            return err
        }

    case resp.GetConsumedResponse() != nil:
        ackResp := resp.GetConsumedResponse()
        if !ackResp.GetSuccess() {
            return fmt.Errorf("ack failed for %s: %s", ackResp.GetUuid(), ackResp.GetError().GetMessage())
        }

    case resp.GetError() != nil:
        return fmt.Errorf("consume failed: %s", resp.GetError().GetMessage())
    }
}
```

### 4. Choose Ack, Nack, Retry, or dead-letter behavior

Arke uses the `MessageConsumed` fields this way:

| Client action                             | What to send                                     | Arke behavior                                                       |
| ----------------------------------------- | ------------------------------------------------ | ------------------------------------------------------------------- |
| Ack successful processing                 | `Uuid=<message uuid>, Nack=false`                | Calls provider `Ack`                                                |
| Reject and discard or requeue immediately | `Uuid=<message uuid>, Nack=true, RequeueDelay=0` | Calls `Nack`, unless dead-lettering is configured                   |
| Retry later                               | `Uuid=<message uuid>, Nack=true, RequeueDelay>0` | Calls `Retry` with the delay in seconds                             |
| Dead-letter on failure                    | `Nack=true` and source has `DeadLetterAddress`   | Arke attempts `DeadLetter`, then falls back to `Nack` if that fails |

### 5. Handle `declare_only` when you only want provisioning

If `Source.DeclareOnly` is `true`, Arke declares the source and bindings but
does not deliver messages. The server returns a
`ConsumeResponse{DeclareOnlyResponse: ...}` result instead of normal message
traffic.

This is useful when a client wants to ensure queues, bindings, or stream
consumer structures exist before another process starts consuming.

---

## Step 4: Publish Messages

For low-volume or request/response style publishing, use `PublishOne`.
For a sustained stream of messages, use `Publish`.

### Publishing with `PublishOne`

```go
msg := &pb.Message{
    Headers: map[string]string{
        "content-type": "application/json",
        "event-type":   "order.created",
    },
    Body:       []byte(`{"id":"12345"}`),
    Persistent: true,
    Confirm:    true,
    Address: &pb.Address{
        Name:     "orders.exchange",
        Subjects: []string{"orders.created"},
        Type:     pb.Address_TOPIC,
    },
}

pubResp, err := producer.PublishOne(ctx, msg)
if err != nil {
    return err
}
if !pubResp.GetSuccess() {
    return fmt.Errorf("publish failed: %s", pubResp.GetError().GetMessage())
}
```

Publishing rules that matter:

- `Message.Address.Subjects` must contain exactly one subject for publish calls.
- `Persistent` requests durable broker storage when supported by the provider.
- `Confirm` enables broker publisher confirmation behavior when supported.
- `PublishId` and `PublisherName` are used for RabbitMQ Stream
  deduplication and should be supplied only when you need that behavior.

### Publishing with `Publish`

`Publish` is a bidirectional stream.

- The client sends `Message` values.
- Arke sends one `MessageResponse` per message.
- A failed response for one message does not necessarily mean the stream is
  unusable; check `error.IsFatal`.

Minimal pattern:

```go
stream, err := producer.Publish(ctx)
if err != nil {
    return err
}

for _, msg := range batch {
    if err := stream.Send(msg); err != nil {
        return err
    }

    resp, err := stream.Recv()
    if err != nil {
        return err
    }
    if !resp.GetSuccess() {
        return fmt.Errorf("publish failed: %s", resp.GetError().GetMessage())
    }
}

if err := stream.CloseSend(); err != nil {
    return err
}
```

---


## Source Configuration Notes

Arke applies a few server-side behaviors that clients should know about.

### Prefetch default

If `Source.PrefetchCount < 1`, Arke forces it to `1`.

### Auto-delete expiry default

If `Source.AutoDelete` is `true` and `Source.Options["Expires"]` is not set,
Arke adds `Expires=300000` so the queue expires after 5 minutes of disuse.

### Supported source options

For the current `amqp091` provider, the documented `Source.Options` keys are:

| Option              | Meaning                                                               |
| ------------------- | --------------------------------------------------------------------- |
| `MessageTTL`        | Queue message TTL in milliseconds                                     |
| `Expires`           | Queue expiry in milliseconds                                          |
| `DeadLetterAddress` | Exchange used for dead-lettering                                      |
| `DeadLetterSubject` | Routing key used for dead-lettering                                   |
| `Offset`            | Stream start offset: `first`, `continue`, `next`, or a quoted integer |
| `ConsumerGroup`     | Stream consumer group                                                 |

If the client sends an unsupported option, Arke rejects the subscription before
calling the provider.

### Streams vs queues

For RabbitMQ Streams:

- Use `Source.Type = STREAM`.
- Use `Address.Type = STREAM` when applicable.
- `Offset` and `ConsumerGroup` apply to stream subscriptions.
- `SingleActiveConsumer` on a stream must be paired with
  `ConsumerGroup`.
- Stream sources do not support all queue-oriented options.

---

## Error Handling Expectations

Arke surfaces errors in two ways:

- As gRPC call errors.
- Inside protobuf responses through `pb.Error`.

Check both.

Important conventions:

- A missing prior `Connect` causes publish or consume operations to fail because
  Arke cannot find connection information for that client.
- `pb.Error.IsFatal() = true` means the session should be treated as broken and
  reconnected.
- `pb.Error.IsFatal() = false` means the failure was message-scoped or otherwise
  non-fatal.
- `PublishOne` and `Publish` reject messages with zero or multiple publish
  subjects.

---

## Shutdown Sequence

On a clean shutdown:

1. Stop accepting new work in the client.
2. Finish or cancel in-flight publish and consume processing.
3. Call `Producer.Disconnect` or `Consumer.Disconnect`.
4. Close the gRPC client connection.

If the client process dies without calling `Disconnect`, Arke eventually cleans
up stale session state in the background.

---

## Complete Go Example

This condensed example uses separate producer and consumer connections.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "log"
    "time"

    pb "github.com/sassoftware/arke/api"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    producerConn, err := grpc.NewClient(
        "localhost:50051",
        grpc.WithTransportCredentials(insecure.NewCredentials()),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer producerConn.Close()

    consumerConn, err := grpc.NewClient(
        "localhost:50051",
        grpc.WithTransportCredentials(insecure.NewCredentials()),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer consumerConn.Close()

    producer := pb.NewProducerClient(producerConn)
    consumer := pb.NewConsumerClient(consumerConn)

    producerCfg := &pb.ConnectionConfiguration{
        Host:       "rabbitmq.internal",
        Port:       5672,
        Provider:   "amqp091",
        ClientName: "example-producer",
        Credentials: &pb.Credentials{
            Username: "guest",
            Password: "guest",
        },
    }
    consumerCfg := &pb.ConnectionConfiguration{
        Host:       "rabbitmq.internal",
        Port:       5672,
        Provider:   "amqp091",
        ClientName: "example-consumer",
        Credentials: &pb.Credentials{
            Username: "guest",
            Password: "guest",
        },
    }

    if resp, err := producer.Connect(ctx, producerCfg); err != nil ||
            !resp.GetSuccess() {
        log.Fatalf("producer connect failed: %v %v", err, resp.GetError())
    }
    defer producer.Disconnect(ctx, &pb.Empty{})

    if resp, err := consumer.Connect(ctx, consumerCfg); err != nil ||
            !resp.GetSuccess() {
        log.Fatalf("consumer connect failed: %v %v", err, resp.GetError())
    }
    defer consumer.Disconnect(ctx, &pb.Empty{})

    consumeStream, err := consumer.Consume(ctx)
    if err != nil {
        log.Fatal(err)
    }

    source := &pb.Source{
        Name: "orders-worker",
        Address: &pb.Address{
            Name:     "orders.exchange",
            Subjects: []string{"orders.created"},
            Type:     pb.Address_TOPIC,
        },
        Type:          pb.Source_QUEUE,
        PrefetchCount: 5,
        Options: map[string]string{
            "DeadLetterAddress": "orders.dlx",
            "DeadLetterSubject": "orders.failed",
        },
    }

    if err := consumeStream.Send(&pb.Consume{
        Msg: &pb.Consume_Src{Src: source},
    }); err != nil {
        log.Fatal(err)
    }

    go func() {
        for {
            resp, err := consumeStream.Recv()
            if err == io.EOF {
                return
            }
            if err != nil {
                log.Printf("consume recv failed: %v", err)
                return
            }

            if resp.GetMsg() == nil {
                continue
            }

            msg := resp.GetMsg()
            fmt.Printf("received: %s\n", string(msg.GetBody()))

            ack := &pb.MessageConsumed{Uuid: msg.GetUuid()}
            if err := consumeStream.Send(&pb.Consume{
                Msg: &pb.Consume_Ack{Ack: ack},
            }); err != nil {
                log.Printf("ack send failed: %v", err)
                return
            }
        }
    }()

    pubResp, err := producer.PublishOne(ctx, &pb.Message{
        Body:       []byte(`{"id":"12345"}`),
        Persistent: true,
        Confirm:    true,
        Address: &pb.Address{
            Name:     "orders.exchange",
            Subjects: []string{"orders.created"},
            Type:     pb.Address_TOPIC,
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    if !pubResp.GetSuccess() {
        log.Fatalf("publish failed: %s", pubResp.GetError().GetMessage())
    }

    time.Sleep(2 * time.Second)
}
```

---

## Related Documents

- [Protocol Reference](../arke_protocol.md)
- [Connection and Message Lifecycle](connection-message-lifecycle.md)
- [Architecture Overview](architecture-overview.md)
- [Deployment and Operations Runbook](deployment-operations-runbook.md)
