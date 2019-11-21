package org.arke;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 * <pre>
 * Service for consuming messages
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.25.0)",
    comments = "Source: arke.proto")
public final class ConsumerGrpc {

  private ConsumerGrpc() {}

  public static final String SERVICE_NAME = "arke.Consumer";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.arke.ArkeProtos.ConnectionConfiguration,
      org.arke.ArkeProtos.ConnectResponse> getConnectMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Connect",
      requestType = org.arke.ArkeProtos.ConnectionConfiguration.class,
      responseType = org.arke.ArkeProtos.ConnectResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.arke.ArkeProtos.ConnectionConfiguration,
      org.arke.ArkeProtos.ConnectResponse> getConnectMethod() {
    io.grpc.MethodDescriptor<org.arke.ArkeProtos.ConnectionConfiguration, org.arke.ArkeProtos.ConnectResponse> getConnectMethod;
    if ((getConnectMethod = ConsumerGrpc.getConnectMethod) == null) {
      synchronized (ConsumerGrpc.class) {
        if ((getConnectMethod = ConsumerGrpc.getConnectMethod) == null) {
          ConsumerGrpc.getConnectMethod = getConnectMethod =
              io.grpc.MethodDescriptor.<org.arke.ArkeProtos.ConnectionConfiguration, org.arke.ArkeProtos.ConnectResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Connect"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.arke.ArkeProtos.ConnectionConfiguration.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.arke.ArkeProtos.ConnectResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ConsumerMethodDescriptorSupplier("Connect"))
              .build();
        }
      }
    }
    return getConnectMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.arke.ArkeProtos.Source,
      org.arke.ArkeProtos.Message> getSubscribeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Subscribe",
      requestType = org.arke.ArkeProtos.Source.class,
      responseType = org.arke.ArkeProtos.Message.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<org.arke.ArkeProtos.Source,
      org.arke.ArkeProtos.Message> getSubscribeMethod() {
    io.grpc.MethodDescriptor<org.arke.ArkeProtos.Source, org.arke.ArkeProtos.Message> getSubscribeMethod;
    if ((getSubscribeMethod = ConsumerGrpc.getSubscribeMethod) == null) {
      synchronized (ConsumerGrpc.class) {
        if ((getSubscribeMethod = ConsumerGrpc.getSubscribeMethod) == null) {
          ConsumerGrpc.getSubscribeMethod = getSubscribeMethod =
              io.grpc.MethodDescriptor.<org.arke.ArkeProtos.Source, org.arke.ArkeProtos.Message>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Subscribe"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.arke.ArkeProtos.Source.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.arke.ArkeProtos.Message.getDefaultInstance()))
              .setSchemaDescriptor(new ConsumerMethodDescriptorSupplier("Subscribe"))
              .build();
        }
      }
    }
    return getSubscribeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.arke.ArkeProtos.Message,
      org.arke.ArkeProtos.AckResponse> getAckMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AckMessage",
      requestType = org.arke.ArkeProtos.Message.class,
      responseType = org.arke.ArkeProtos.AckResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.arke.ArkeProtos.Message,
      org.arke.ArkeProtos.AckResponse> getAckMessageMethod() {
    io.grpc.MethodDescriptor<org.arke.ArkeProtos.Message, org.arke.ArkeProtos.AckResponse> getAckMessageMethod;
    if ((getAckMessageMethod = ConsumerGrpc.getAckMessageMethod) == null) {
      synchronized (ConsumerGrpc.class) {
        if ((getAckMessageMethod = ConsumerGrpc.getAckMessageMethod) == null) {
          ConsumerGrpc.getAckMessageMethod = getAckMessageMethod =
              io.grpc.MethodDescriptor.<org.arke.ArkeProtos.Message, org.arke.ArkeProtos.AckResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AckMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.arke.ArkeProtos.Message.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.arke.ArkeProtos.AckResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ConsumerMethodDescriptorSupplier("AckMessage"))
              .build();
        }
      }
    }
    return getAckMessageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.arke.ArkeProtos.Message,
      org.arke.ArkeProtos.NackResponse> getNackMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "NackMessage",
      requestType = org.arke.ArkeProtos.Message.class,
      responseType = org.arke.ArkeProtos.NackResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.arke.ArkeProtos.Message,
      org.arke.ArkeProtos.NackResponse> getNackMessageMethod() {
    io.grpc.MethodDescriptor<org.arke.ArkeProtos.Message, org.arke.ArkeProtos.NackResponse> getNackMessageMethod;
    if ((getNackMessageMethod = ConsumerGrpc.getNackMessageMethod) == null) {
      synchronized (ConsumerGrpc.class) {
        if ((getNackMessageMethod = ConsumerGrpc.getNackMessageMethod) == null) {
          ConsumerGrpc.getNackMessageMethod = getNackMessageMethod =
              io.grpc.MethodDescriptor.<org.arke.ArkeProtos.Message, org.arke.ArkeProtos.NackResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "NackMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.arke.ArkeProtos.Message.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.arke.ArkeProtos.NackResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ConsumerMethodDescriptorSupplier("NackMessage"))
              .build();
        }
      }
    }
    return getNackMessageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.arke.ArkeProtos.Empty,
      org.arke.ArkeProtos.Empty> getDisconnectMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Disconnect",
      requestType = org.arke.ArkeProtos.Empty.class,
      responseType = org.arke.ArkeProtos.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.arke.ArkeProtos.Empty,
      org.arke.ArkeProtos.Empty> getDisconnectMethod() {
    io.grpc.MethodDescriptor<org.arke.ArkeProtos.Empty, org.arke.ArkeProtos.Empty> getDisconnectMethod;
    if ((getDisconnectMethod = ConsumerGrpc.getDisconnectMethod) == null) {
      synchronized (ConsumerGrpc.class) {
        if ((getDisconnectMethod = ConsumerGrpc.getDisconnectMethod) == null) {
          ConsumerGrpc.getDisconnectMethod = getDisconnectMethod =
              io.grpc.MethodDescriptor.<org.arke.ArkeProtos.Empty, org.arke.ArkeProtos.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Disconnect"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.arke.ArkeProtos.Empty.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.arke.ArkeProtos.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new ConsumerMethodDescriptorSupplier("Disconnect"))
              .build();
        }
      }
    }
    return getDisconnectMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ConsumerStub newStub(io.grpc.Channel channel) {
    return new ConsumerStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ConsumerBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new ConsumerBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ConsumerFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new ConsumerFutureStub(channel);
  }

  /**
   * <pre>
   * Service for consuming messages
   * </pre>
   */
  public static abstract class ConsumerImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Connect to a message broker. Pass in a ConnectionConfiguration with broker
     * specific connection information.
     * </pre>
     */
    public void connect(org.arke.ArkeProtos.ConnectionConfiguration request,
        io.grpc.stub.StreamObserver<org.arke.ArkeProtos.ConnectResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getConnectMethod(), responseObserver);
    }

    /**
     * <pre>
     * Subscribe to a message broker source and receive a stream of messages when they are available.
     * </pre>
     */
    public void subscribe(org.arke.ArkeProtos.Source request,
        io.grpc.stub.StreamObserver<org.arke.ArkeProtos.Message> responseObserver) {
      asyncUnimplementedUnaryCall(getSubscribeMethod(), responseObserver);
    }

    /**
     * <pre>
     * Ack a received message.
     * </pre>
     */
    public void ackMessage(org.arke.ArkeProtos.Message request,
        io.grpc.stub.StreamObserver<org.arke.ArkeProtos.AckResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getAckMessageMethod(), responseObserver);
    }

    /**
     * <pre>
     * Nack a received message.
     * </pre>
     */
    public void nackMessage(org.arke.ArkeProtos.Message request,
        io.grpc.stub.StreamObserver<org.arke.ArkeProtos.NackResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getNackMessageMethod(), responseObserver);
    }

    /**
     * <pre>
     * Disconnect from the proxy and the message broker.
     * </pre>
     */
    public void disconnect(org.arke.ArkeProtos.Empty request,
        io.grpc.stub.StreamObserver<org.arke.ArkeProtos.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(getDisconnectMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getConnectMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.arke.ArkeProtos.ConnectionConfiguration,
                org.arke.ArkeProtos.ConnectResponse>(
                  this, METHODID_CONNECT)))
          .addMethod(
            getSubscribeMethod(),
            asyncServerStreamingCall(
              new MethodHandlers<
                org.arke.ArkeProtos.Source,
                org.arke.ArkeProtos.Message>(
                  this, METHODID_SUBSCRIBE)))
          .addMethod(
            getAckMessageMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.arke.ArkeProtos.Message,
                org.arke.ArkeProtos.AckResponse>(
                  this, METHODID_ACK_MESSAGE)))
          .addMethod(
            getNackMessageMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.arke.ArkeProtos.Message,
                org.arke.ArkeProtos.NackResponse>(
                  this, METHODID_NACK_MESSAGE)))
          .addMethod(
            getDisconnectMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.arke.ArkeProtos.Empty,
                org.arke.ArkeProtos.Empty>(
                  this, METHODID_DISCONNECT)))
          .build();
    }
  }

  /**
   * <pre>
   * Service for consuming messages
   * </pre>
   */
  public static final class ConsumerStub extends io.grpc.stub.AbstractStub<ConsumerStub> {
    private ConsumerStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ConsumerStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ConsumerStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ConsumerStub(channel, callOptions);
    }

    /**
     * <pre>
     * Connect to a message broker. Pass in a ConnectionConfiguration with broker
     * specific connection information.
     * </pre>
     */
    public void connect(org.arke.ArkeProtos.ConnectionConfiguration request,
        io.grpc.stub.StreamObserver<org.arke.ArkeProtos.ConnectResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getConnectMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Subscribe to a message broker source and receive a stream of messages when they are available.
     * </pre>
     */
    public void subscribe(org.arke.ArkeProtos.Source request,
        io.grpc.stub.StreamObserver<org.arke.ArkeProtos.Message> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(getSubscribeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Ack a received message.
     * </pre>
     */
    public void ackMessage(org.arke.ArkeProtos.Message request,
        io.grpc.stub.StreamObserver<org.arke.ArkeProtos.AckResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getAckMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Nack a received message.
     * </pre>
     */
    public void nackMessage(org.arke.ArkeProtos.Message request,
        io.grpc.stub.StreamObserver<org.arke.ArkeProtos.NackResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getNackMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Disconnect from the proxy and the message broker.
     * </pre>
     */
    public void disconnect(org.arke.ArkeProtos.Empty request,
        io.grpc.stub.StreamObserver<org.arke.ArkeProtos.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getDisconnectMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * Service for consuming messages
   * </pre>
   */
  public static final class ConsumerBlockingStub extends io.grpc.stub.AbstractStub<ConsumerBlockingStub> {
    private ConsumerBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ConsumerBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ConsumerBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ConsumerBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Connect to a message broker. Pass in a ConnectionConfiguration with broker
     * specific connection information.
     * </pre>
     */
    public org.arke.ArkeProtos.ConnectResponse connect(org.arke.ArkeProtos.ConnectionConfiguration request) {
      return blockingUnaryCall(
          getChannel(), getConnectMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Subscribe to a message broker source and receive a stream of messages when they are available.
     * </pre>
     */
    public java.util.Iterator<org.arke.ArkeProtos.Message> subscribe(
        org.arke.ArkeProtos.Source request) {
      return blockingServerStreamingCall(
          getChannel(), getSubscribeMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Ack a received message.
     * </pre>
     */
    public org.arke.ArkeProtos.AckResponse ackMessage(org.arke.ArkeProtos.Message request) {
      return blockingUnaryCall(
          getChannel(), getAckMessageMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Nack a received message.
     * </pre>
     */
    public org.arke.ArkeProtos.NackResponse nackMessage(org.arke.ArkeProtos.Message request) {
      return blockingUnaryCall(
          getChannel(), getNackMessageMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Disconnect from the proxy and the message broker.
     * </pre>
     */
    public org.arke.ArkeProtos.Empty disconnect(org.arke.ArkeProtos.Empty request) {
      return blockingUnaryCall(
          getChannel(), getDisconnectMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * Service for consuming messages
   * </pre>
   */
  public static final class ConsumerFutureStub extends io.grpc.stub.AbstractStub<ConsumerFutureStub> {
    private ConsumerFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ConsumerFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ConsumerFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ConsumerFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Connect to a message broker. Pass in a ConnectionConfiguration with broker
     * specific connection information.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.arke.ArkeProtos.ConnectResponse> connect(
        org.arke.ArkeProtos.ConnectionConfiguration request) {
      return futureUnaryCall(
          getChannel().newCall(getConnectMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Ack a received message.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.arke.ArkeProtos.AckResponse> ackMessage(
        org.arke.ArkeProtos.Message request) {
      return futureUnaryCall(
          getChannel().newCall(getAckMessageMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Nack a received message.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.arke.ArkeProtos.NackResponse> nackMessage(
        org.arke.ArkeProtos.Message request) {
      return futureUnaryCall(
          getChannel().newCall(getNackMessageMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Disconnect from the proxy and the message broker.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.arke.ArkeProtos.Empty> disconnect(
        org.arke.ArkeProtos.Empty request) {
      return futureUnaryCall(
          getChannel().newCall(getDisconnectMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CONNECT = 0;
  private static final int METHODID_SUBSCRIBE = 1;
  private static final int METHODID_ACK_MESSAGE = 2;
  private static final int METHODID_NACK_MESSAGE = 3;
  private static final int METHODID_DISCONNECT = 4;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ConsumerImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ConsumerImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CONNECT:
          serviceImpl.connect((org.arke.ArkeProtos.ConnectionConfiguration) request,
              (io.grpc.stub.StreamObserver<org.arke.ArkeProtos.ConnectResponse>) responseObserver);
          break;
        case METHODID_SUBSCRIBE:
          serviceImpl.subscribe((org.arke.ArkeProtos.Source) request,
              (io.grpc.stub.StreamObserver<org.arke.ArkeProtos.Message>) responseObserver);
          break;
        case METHODID_ACK_MESSAGE:
          serviceImpl.ackMessage((org.arke.ArkeProtos.Message) request,
              (io.grpc.stub.StreamObserver<org.arke.ArkeProtos.AckResponse>) responseObserver);
          break;
        case METHODID_NACK_MESSAGE:
          serviceImpl.nackMessage((org.arke.ArkeProtos.Message) request,
              (io.grpc.stub.StreamObserver<org.arke.ArkeProtos.NackResponse>) responseObserver);
          break;
        case METHODID_DISCONNECT:
          serviceImpl.disconnect((org.arke.ArkeProtos.Empty) request,
              (io.grpc.stub.StreamObserver<org.arke.ArkeProtos.Empty>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class ConsumerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ConsumerBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.arke.ArkeProtos.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Consumer");
    }
  }

  private static final class ConsumerFileDescriptorSupplier
      extends ConsumerBaseDescriptorSupplier {
    ConsumerFileDescriptorSupplier() {}
  }

  private static final class ConsumerMethodDescriptorSupplier
      extends ConsumerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ConsumerMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (ConsumerGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ConsumerFileDescriptorSupplier())
              .addMethod(getConnectMethod())
              .addMethod(getSubscribeMethod())
              .addMethod(getAckMessageMethod())
              .addMethod(getNackMessageMethod())
              .addMethod(getDisconnectMethod())
              .build();
        }
      }
    }
    return result;
  }
}
