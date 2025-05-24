package org.demo;

import com.google.protobuf.*;
import com.google.protobuf.Descriptors.*;
import com.google.protobuf.util.JsonFormat;
import io.grpc.*;
import io.grpc.MethodDescriptor;
import io.grpc.reflection.v1alpha.ServerReflectionGrpc;
import io.grpc.reflection.v1alpha.ServerReflectionRequest;
import io.grpc.reflection.v1alpha.ServerReflectionResponse;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class GrpcDynamicClient {
    private static final Logger logger = Logger.getLogger(GrpcDynamicClient.class.getName());
    
    private final ManagedChannel channel;
    private final Map<String, FileDescriptor> fileDescriptorCache;
    private final Map<String, Descriptor> messageDescriptorCache;
    private final GrpcParameters parameters;
    private final JsonFormat.Printer jsonPrinter;
    private final JsonFormat.Parser jsonParser;

    public GrpcDynamicClient(GrpcParameters parameters) throws IOException, DescriptorValidationException {
        this.parameters = parameters;
        this.fileDescriptorCache = new ConcurrentHashMap<>();
        this.messageDescriptorCache = new ConcurrentHashMap<>();
        this.jsonPrinter = JsonFormat.printer().includingDefaultValueFields();
        this.jsonParser = JsonFormat.parser().ignoringUnknownFields();
        
        // 从channel池获取channel
        this.channel = GrpcChannelPool.getInstance().getChannel(parameters);
        
        // 初始化标准类型
        initStandardTypes();
        
        // 根据配置选择服务发现方式
        if (parameters.isUseReflection()) {
            initServiceViaReflection();
        } else {
            initServiceViaProtoFile();
        }
    }

    private void initStandardTypes() {
        try {
            // 注册Google标准类型
            registerStandardType("google.protobuf.Timestamp", Timestamp.getDescriptor());
            registerStandardType("google.protobuf.Duration", Duration.getDescriptor());
            registerStandardType("google.protobuf.Any", Any.getDescriptor());
            registerStandardType("google.protobuf.Struct", Struct.getDescriptor());
            registerStandardType("google.protobuf.Value", Value.getDescriptor());
            registerStandardType("google.protobuf.ListValue", ListValue.getDescriptor());
            
            // 注册包装类型
            registerStandardType("google.protobuf.StringValue", StringValue.getDescriptor());
            registerStandardType("google.protobuf.Int32Value", Int32Value.getDescriptor());
            registerStandardType("google.protobuf.Int64Value", Int64Value.getDescriptor());
            registerStandardType("google.protobuf.BoolValue", BoolValue.getDescriptor());
            registerStandardType("google.protobuf.DoubleValue", DoubleValue.getDescriptor());
            registerStandardType("google.protobuf.FloatValue", FloatValue.getDescriptor());
            registerStandardType("google.protobuf.BytesValue", BytesValue.getDescriptor());
            registerStandardType("google.protobuf.UInt32Value", UInt32Value.getDescriptor());
            registerStandardType("google.protobuf.UInt64Value", UInt64Value.getDescriptor());
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize standard types", e);
        }
    }

    private void registerStandardType(String fullName, Descriptor descriptor) {
        messageDescriptorCache.put(fullName, descriptor);
    }

    private void initServiceViaReflection() {
        ServerReflectionGrpc.ServerReflectionStub reflectionStub = ServerReflectionGrpc.newStub(channel);
        
        StreamObserver<ServerReflectionResponse> responseObserver = new StreamObserver<ServerReflectionResponse>() {
            @Override
            public void onNext(ServerReflectionResponse response) {
                try {
                    if (response.hasFileDescriptorResponse()) {
                        for (ByteString fileDescriptorProto : response.getFileDescriptorResponse().getFileDescriptorProtoList()) {
                            FileDescriptor fileDescriptor = FileDescriptor.buildFrom(
                                    DescriptorProtos.FileDescriptorProto.parseFrom(fileDescriptorProto),
                                new FileDescriptor[0]
                            );
                            registerProtoFile(fileDescriptor);
                        }
                    }
                } catch (Exception e) {
                    logger.severe("Error processing reflection response: " + e.getMessage());
                }
            }

            @Override
            public void onError(Throwable t) {
                logger.severe("Reflection error: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                logger.info("Reflection completed");
            }
        };

        StreamObserver<ServerReflectionRequest> requestObserver = reflectionStub.serverReflectionInfo(responseObserver);
        requestObserver.onNext(ServerReflectionRequest.newBuilder()
                .setListServices("")
                .build());
        requestObserver.onCompleted();
    }

    private void initServiceViaProtoFile() throws IOException, DescriptorValidationException {
        if (parameters.getProtoFilePath() != null) {
            // 解析单个proto文件
            FileDescriptor descriptor = ProtoFileParser.parseProtoFile(parameters.getProtoFilePath());
            registerProtoFile(descriptor);
        } else {
            throw new IllegalStateException("No proto file provided");
        }
    }

    private void registerProtoFile(FileDescriptor fileDescriptor) {
        String packageName = fileDescriptor.getPackage();
        fileDescriptorCache.put(packageName, fileDescriptor);
        
        for (Descriptor descriptor : fileDescriptor.getMessageTypes()) {
            messageDescriptorCache.put(packageName + "." + descriptor.getName(), descriptor);
        }
    }

    public DynamicMessage callMethod(String serviceName, String methodName, DynamicMessage request) {
        FileDescriptor fileDescriptor = findFileDescriptor(serviceName);
        if (fileDescriptor == null) {
            throw new IllegalArgumentException("Service not found: " + serviceName);
        }

        com.google.protobuf.Descriptors.ServiceDescriptor serviceDescriptor = fileDescriptor.findServiceByName(serviceName);
        if (serviceDescriptor == null) {
            throw new IllegalArgumentException("Service not found: " + serviceName);
        }

        com.google.protobuf.Descriptors.MethodDescriptor methodDescriptor = serviceDescriptor.findMethodByName(methodName);
        if (methodDescriptor == null) {
            throw new IllegalArgumentException("Method not found: " + methodName);
        }

        if (methodDescriptor.isClientStreaming() || methodDescriptor.isServerStreaming()) {
            throw new UnsupportedOperationException("Streaming RPC is not supported yet");
        }

        MethodDescriptor<DynamicMessage, DynamicMessage> method = MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName, methodName))
                .setRequestMarshaller(new DynamicMessageMarshaller(methodDescriptor.getInputType()))
                .setResponseMarshaller(new DynamicMessageMarshaller(methodDescriptor.getOutputType()))
                .build();

        CallOptions callOptions = CallOptions.DEFAULT
                .withDeadlineAfter(parameters.getTimeout(), parameters.getTimeoutUnit());

        return ClientCalls.blockingUnaryCall(
            channel.newCall(method, callOptions),
            request
        );
    }

    private FileDescriptor findFileDescriptor(String serviceName) {
        for (FileDescriptor fd : fileDescriptorCache.values()) {
            if (fd.findServiceByName(serviceName) != null) {
                return fd;
            }
        }
        return null;
    }

    public DynamicMessage createMessage(String messageTypeName, Map<String, Object> fields) {
        Descriptor descriptor = messageDescriptorCache.get(messageTypeName);
        if (descriptor == null) {
            throw new IllegalArgumentException("Message type not found: " + messageTypeName);
        }

        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            FieldDescriptor fieldDescriptor = descriptor.findFieldByName(entry.getKey());
            if (fieldDescriptor == null) {
                throw new IllegalArgumentException("Field not found: " + entry.getKey());
            }
            setField(builder, fieldDescriptor, entry.getValue());
        }
        return builder.build();
    }

    private void setField(DynamicMessage.Builder builder, FieldDescriptor fieldDescriptor, Object value) {
        if (fieldDescriptor.isRepeated()) {
            if (!(value instanceof Iterable)) {
                throw new IllegalArgumentException("Repeated field requires Iterable value");
            }
            for (Object item : (Iterable<?>) value) {
                builder.addRepeatedField(fieldDescriptor, convertValue(fieldDescriptor, item));
            }
        } else {
            builder.setField(fieldDescriptor, convertValue(fieldDescriptor, value));
        }
    }

    private Object convertValue(FieldDescriptor fieldDescriptor, Object value) {
        if (value == null) {
            return null;
        }

        switch (fieldDescriptor.getType()) {
            case MESSAGE:
                if (value instanceof DynamicMessage) {
                    return value;
                }
                if (value instanceof Map) {
                    return createMessage(fieldDescriptor.getMessageType().getFullName(), (Map<String, Object>) value);
                }
                throw new IllegalArgumentException("Message field requires DynamicMessage or Map value");
            case ENUM:
                if (value instanceof Number) {
                    return fieldDescriptor.getEnumType().findValueByNumber(((Number) value).intValue());
                }
                if (value instanceof String) {
                    return fieldDescriptor.getEnumType().findValueByName((String) value);
                }
                throw new IllegalArgumentException("Enum field requires Number or String value");
            default:
                return value;
        }
    }

    public String messageToJson(DynamicMessage message) {
        try {
            return jsonPrinter.print(message);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert message to JSON", e);
        }
    }

    public DynamicMessage jsonToMessage(String json, String messageTypeName) {
        try {
            Descriptor descriptor = messageDescriptorCache.get(messageTypeName);
            if (descriptor == null) {
                throw new IllegalArgumentException("Message type not found: " + messageTypeName);
            }
            
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
            jsonParser.merge(json, builder);
            return builder.build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert JSON to message", e);
        }
    }

    public void shutdown() {
        GrpcChannelPool.getInstance().shutdown();
        logger.info("GrpcDynamicClient shutdown");
    }

    private static class DynamicMessageMarshaller implements MethodDescriptor.Marshaller<DynamicMessage> {
        private final Descriptor messageDescriptor;

        public DynamicMessageMarshaller(Descriptor messageDescriptor) {
            this.messageDescriptor = messageDescriptor;
        }

        @Override
        public DynamicMessage parse(InputStream stream) {
            try {
                return DynamicMessage.parseFrom(messageDescriptor, stream);
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse message", e);
            }
        }

        @Override
        public InputStream stream(DynamicMessage message) {
            return message.toByteString().newInput();
        }
    }
}