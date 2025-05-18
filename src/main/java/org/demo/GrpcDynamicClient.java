package org.demo;

import com.google.protobuf.*;
import com.google.protobuf.Descriptors.*;
import io.grpc.*;
import io.grpc.stub.ClientCalls;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class GrpcDynamicClient {
    private final ManagedChannel channel;
    private final Map<String, FileDescriptor> fileDescriptorCache;
    private final Map<String, Descriptor> messageDescriptorCache;

    public GrpcDynamicClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.fileDescriptorCache = new ConcurrentHashMap<>();
        this.messageDescriptorCache = new ConcurrentHashMap<>();
        initStandardTypes();
    }

    private void initStandardTypes() {
        // 初始化标准类型描述符
        try {
            Descriptor timestampDescriptor = com.google.protobuf.Timestamp.getDescriptor();
            messageDescriptorCache.put("google.protobuf.Timestamp", timestampDescriptor);
            
            Descriptor durationDescriptor = com.google.protobuf.Duration.getDescriptor();
            messageDescriptorCache.put("google.protobuf.Duration", durationDescriptor);
            
            Descriptor anyDescriptor = com.google.protobuf.Any.getDescriptor();
            messageDescriptorCache.put("google.protobuf.Any", anyDescriptor);
            
            Descriptor structDescriptor = com.google.protobuf.Struct.getDescriptor();
            messageDescriptorCache.put("google.protobuf.Struct", structDescriptor);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize standard types", e);
        }
    }

    public void registerProtoFile(FileDescriptor fileDescriptor) {
        String packageName = fileDescriptor.getPackage();
        fileDescriptorCache.put(packageName, fileDescriptor);
        
        // 缓存所有消息类型描述符
        for (Descriptor descriptor : fileDescriptor.getMessageTypes()) {
            messageDescriptorCache.put(packageName + "." + descriptor.getName(), descriptor);
        }
    }

    public DynamicMessage callMethod(String serviceName, String methodName, DynamicMessage request) {
        FileDescriptor fileDescriptor = findFileDescriptor(serviceName);
        if (fileDescriptor == null) {
            throw new IllegalArgumentException("Service not found: " + serviceName);
        }

        ServiceDescriptor serviceDescriptor = fileDescriptor.findServiceByName(serviceName);
        if (serviceDescriptor == null) {
            throw new IllegalArgumentException("Service not found: " + serviceName);
        }

        MethodDescriptor methodDescriptor = serviceDescriptor.findMethodByName(methodName);
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

        return ClientCalls.blockingUnaryCall(channel, method, request);
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

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    private static class DynamicMessageMarshaller implements MethodDescriptor.Marshaller<DynamicMessage> {
        private final Descriptor messageDescriptor;

        public DynamicMessageMarshaller(Descriptor messageDescriptor) {
            this.messageDescriptor = messageDescriptor;
        }

        @Override
        public DynamicMessage parse(io.grpc.MethodDescriptor.ProtocolBufferInputStream stream) {
            try {
                return DynamicMessage.parseFrom(messageDescriptor, stream);
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse message", e);
            }
        }

        @Override
        public io.grpc.MethodDescriptor.ProtocolBufferInputStream stream(DynamicMessage message) {
            return new io.grpc.MethodDescriptor.ProtocolBufferInputStream(message.toByteString().newInput());
        }
    }
}