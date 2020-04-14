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
 * Service for producing messages
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.28.1)",
    comments = "Source: arke.proto")
public final class ProducerGrpc {

  private ProducerGrpc() {}

  public static final String SERVICE_NAME = "arke.Producer";

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
    if ((getConnectMethod = ProducerGrpc.getConnectMethod) == null) {
      synchronized (ProducerGrpc.class) {
        if ((getConnectMethod = ProducerGrpc.getConnectMethod) == null) {
          ProducerGrpc.getConnectMethod = getConnectMethod =
              io.grpc.MethodDescriptor.<org.arke.ArkeProtos.ConnectionConfiguration, org.arke.ArkeProtos.ConnectResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Connect"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.arke.ArkeProtos.ConnectionConfiguration.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.arke.ArkeProtos.ConnectResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ProducerMethodDescriptorSupplier("Connect"))
              .build();
        }
      }
    }
    return getConnectMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.arke.ArkeProtos.Message,
      org.arke.ArkeProtos.MessageResponse> getPublishMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Publish",
      requestType = org.arke.ArkeProtos.Message.class,
      responseType = org.arke.ArkeProtos.MessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<org.arke.ArkeProtos.Message,
      org.arke.ArkeProtos.MessageResponse> getPublishMethod() {
    io.grpc.MethodDescriptor<org.arke.ArkeProtos.Message, org.arke.ArkeProtos.MessageResponse> getPublishMethod;
    if ((getPublishMethod = ProducerGrpc.getPublishMethod) == null) {
      synchronized (ProducerGrpc.class) {
        if ((getPublishMethod = ProducerGrpc.getPublishMethod) == null) {
          ProducerGrpc.getPublishMethod = getPublishMethod =
              io.grpc.MethodDescriptor.<org.arke.ArkeProtos.Message, org.arke.ArkeProtos.MessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Publish"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.arke.ArkeProtos.Message.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.arke.ArkeProtos.MessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ProducerMethodDescriptorSupplier("Publish"))
              .build();
        }
      }
    }
    return getPublishMethod;
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
    if ((getDisconnectMethod = ProducerGrpc.getDisconnectMethod) == null) {
      synchronized (ProducerGrpc.class) {
        if ((getDisconnectMethod = ProducerGrpc.getDisconnectMethod) == null) {
          ProducerGrpc.getDisconnectMethod = getDisconnectMethod =
              io.grpc.MethodDescriptor.<org.arke.ArkeProtos.Empty, org.arke.ArkeProtos.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Disconnect"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.arke.ArkeProtos.Empty.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.arke.ArkeProtos.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new ProducerMethodDescriptorSupplier("Disconnect"))
              .build();
        }
      }
    }
    return getDisconnectMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ProducerStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ProducerStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ProducerStub>() {
        @java.lang.Override
        public ProducerStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ProducerStub(channel, callOptions);
        }
      };
    return ProducerStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ProducerBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ProducerBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ProducerBlockingStub>() {
        @java.lang.Override
        public ProducerBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ProducerBlockingStub(channel, callOptions);
        }
      };
    return ProducerBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ProducerFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ProducerFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ProducerFutureStub>() {
        @java.lang.Override
        public ProducerFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ProducerFutureStub(channel, callOptions);
        }
      };
    return ProducerFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * Service for producing messages
   * </pre>
   */
  public static abstract class ProducerImplBase implements io.grpc.BindableService {

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
     * Send messages to the message broker.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<org.arke.ArkeProtos.Message> publish(
        io.grpc.stub.StreamObserver<org.arke.ArkeProtos.MessageResponse> responseObserver) {
      return asyncUnimplementedStreamingCall(getPublishMethod(), responseObserver);
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
            getPublishMethod(),
            asyncBidiStreamingCall(
              new MethodHandlers<
                org.arke.ArkeProtos.Message,
                org.arke.ArkeProtos.MessageResponse>(
                  this, METHODID_PUBLISH)))
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
   * Service for producing messages
   * </pre>
   */
  public static final class ProducerStub extends io.grpc.stub.AbstractAsyncStub<ProducerStub> {
    private ProducerStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ProducerStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ProducerStub(channel, callOptions);
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
     * Send messages to the message broker.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<org.arke.ArkeProtos.Message> publish(
        io.grpc.stub.StreamObserver<org.arke.ArkeProtos.MessageResponse> responseObserver) {
      return asyncBidiStreamingCall(
          getChannel().newCall(getPublishMethod(), getCallOptions()), responseObserver);
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
   * Service for producing messages
   * </pre>
   */
  public static final class ProducerBlockingStub extends io.grpc.stub.AbstractBlockingStub<ProducerBlockingStub> {
    private ProducerBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ProducerBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ProducerBlockingStub(channel, callOptions);
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
   * Service for producing messages
   * </pre>
   */
  public static final class ProducerFutureStub extends io.grpc.stub.AbstractFutureStub<ProducerFutureStub> {
    private ProducerFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ProducerFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ProducerFutureStub(channel, callOptions);
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
  private static final int METHODID_DISCONNECT = 1;
  private static final int METHODID_PUBLISH = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ProducerImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ProducerImplBase serviceImpl, int methodId) {
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
        case METHODID_PUBLISH:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.publish(
              (io.grpc.stub.StreamObserver<org.arke.ArkeProtos.MessageResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class ProducerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ProducerBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.arke.ArkeProtos.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Producer");
    }
  }

  private static final class ProducerFileDescriptorSupplier
      extends ProducerBaseDescriptorSupplier {
    ProducerFileDescriptorSupplier() {}
  }

  private static final class ProducerMethodDescriptorSupplier
      extends ProducerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ProducerMethodDescriptorSupplier(String methodName) {
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
      synchronized (ProducerGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ProducerFileDescriptorSupplier())
              .addMethod(getConnectMethod())
              .addMethod(getPublishMethod())
              .addMethod(getDisconnectMethod())
              .build();
        }
      }
    }
    return result;
  }
}
