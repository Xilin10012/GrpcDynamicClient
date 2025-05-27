package org.demo;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors; // 通用导入
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
// 特指 Protobuf 的 MethodDescriptor
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
// 特指 gRPC 的 MethodDescriptor
import io.grpc.reflection.v1alpha.ServerReflectionGrpc;
import io.grpc.reflection.v1alpha.ServerReflectionRequest;
import io.grpc.reflection.v1alpha.ServerReflectionResponse;
import io.grpc.reflection.v1alpha.ServiceResponse; // 修正 ServiceResponse 的导入
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import io.grpc.StatusRuntimeException;


import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class GrpcDynamicClient {
    private static final Logger logger = Logger.getLogger(GrpcDynamicClient.class.getName());

    private final ManagedChannel channel;
    private final Map<String, FileDescriptor> fileDescriptorMapByName;
    private final Map<String, ServiceDescriptor> serviceDescriptorCache;
    private final Map<String, Descriptor> messageDescriptorCache;

    private final GrpcParameters parameters;
    private final JsonFormat.Printer jsonPrinter;
    private final JsonFormat.Parser jsonParser;

    public GrpcDynamicClient(GrpcParameters parameters) throws IOException, DescriptorValidationException {
        this.parameters = parameters;
        this.fileDescriptorMapByName = new ConcurrentHashMap<>();
        this.serviceDescriptorCache = new ConcurrentHashMap<>();
        this.messageDescriptorCache = new ConcurrentHashMap<>();

        this.jsonPrinter = JsonFormat.printer().includingDefaultValueFields().preservingProtoFieldNames();
        this.jsonParser = JsonFormat.parser().ignoringUnknownFields();

        this.channel = GrpcChannelPool.getInstance().getChannel(parameters);

        if (parameters.isUseReflection()) {
            logger.info("通过反射初始化服务定义...");
            CountDownLatch reflectionLatch = new CountDownLatch(1);
            initServiceViaReflection(reflectionLatch);
            try {
                long timeout = parameters.getTimeout() > 0 ? parameters.getTimeout() : 30;
                TimeUnit timeoutUnit = parameters.getTimeoutUnit() != null ? parameters.getTimeoutUnit() : TimeUnit.SECONDS;
                logger.fine("等待反射完成，超时时间: " + timeout + " " + timeoutUnit.name());
                if (!reflectionLatch.await(timeout, timeoutUnit)) {
                    logger.log(Level.WARNING, "gRPC 反射未在超时期限内完成 (" + timeout + " " + timeoutUnit.name() + ")。服务发现可能不完整。");
                } else {
                    logger.info("gRPC 反射已完成。");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.log(Level.SEVERE, "gRPC 反射被中断。", e);
                throw new IOException("gRPC 反射过程被中断。", e);
            }
        } else if (parameters.getProtoFilePath() != null && !parameters.getProtoFilePath().isEmpty()) {
            logger.info("通过 proto 文件初始化服务定义: " + parameters.getProtoFilePath());
            FileDescriptor descriptor = ProtoFileParser.parseProtoFile(parameters.getProtoFilePath());
            registerFileDescriptor(descriptor);
        } else {
            logger.warning("未指定用于服务发现的反射或 proto 文件路径。");
        }
        logger.info("GrpcDynamicClient 已为目标初始化: " + parameters.getHost() + ":" + parameters.getPort());
    }

    private void initServiceViaReflection(CountDownLatch reflectionLatch) {
        ServerReflectionGrpc.ServerReflectionStub reflectionStub = ServerReflectionGrpc.newStub(channel);
        final List<DescriptorProtos.FileDescriptorProto> fileDescriptorProtos = new ArrayList<>();

        StreamObserver<ServerReflectionResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(ServerReflectionResponse response) {
                if (response.getMessageResponseCase() == ServerReflectionResponse.MessageResponseCase.FILE_DESCRIPTOR_RESPONSE) {
                    logger.fine("收到 FileDescriptorResponse，包含 " + response.getFileDescriptorResponse().getFileDescriptorProtoList().size() + " 个 proto。");
                    fileDescriptorProtos.addAll(response.getFileDescriptorResponse().getFileDescriptorProtoList().stream()
                            .map(bs -> {
                                try {
                                    return DescriptorProtos.FileDescriptorProto.parseFrom(bs);
                                } catch (InvalidProtocolBufferException e) {
                                    logger.log(Level.WARNING, "从 ByteString 解析 FileDescriptorProto 失败", e);
                                    return null;
                                }
                            })
                            .filter(fdp -> fdp != null)
                            .collect(Collectors.toList()));
                } else if (response.getMessageResponseCase() == ServerReflectionResponse.MessageResponseCase.LIST_SERVICES_RESPONSE) {
                    logger.fine("收到 ListServicesResponse，服务列表: " +
                            response.getListServicesResponse().getServiceList().stream()
                                    .map(ServiceResponse::getName) // *** 修正此处的 ServiceResponse ***
                                    .collect(Collectors.joining(", ")));
                } else if (response.getMessageResponseCase() == ServerReflectionResponse.MessageResponseCase.ERROR_RESPONSE) {
                    logger.severe("来自服务器的反射错误: " + response.getErrorResponse().getErrorMessage() + " (错误码: " + response.getErrorResponse().getErrorCode() + ")");
                }
            }

            @Override
            public void onError(Throwable t) {
                logger.log(Level.SEVERE, "gRPC 反射调用失败。", t);
                reflectionLatch.countDown();
            }

            @Override
            public void onCompleted() {
                logger.info("gRPC 反射流已完成。正在处理 " + fileDescriptorProtos.size() + " 个 FileDescriptorProto。");
                try {
                    Map<String, FileDescriptor> currentBatchBuiltDescriptors = new HashMap<>();
                    List<DescriptorProtos.FileDescriptorProto> toProcess = new ArrayList<>(fileDescriptorProtos);
                    int processedInLastPass;
                    do {
                        processedInLastPass = 0;
                        List<DescriptorProtos.FileDescriptorProto> remaining = new ArrayList<>();
                        for (DescriptorProtos.FileDescriptorProto proto : toProcess) {
                            List<FileDescriptor> dependencies = new ArrayList<>();
                            boolean dependenciesMet = true;
                            for (String depName : proto.getDependencyList()) {
                                FileDescriptor depFd = fileDescriptorMapByName.get(depName);
                                if (depFd == null) {
                                    depFd = currentBatchBuiltDescriptors.get(depName);
                                }
                                if (depFd != null) {
                                    dependencies.add(depFd);
                                } else {
                                    if (!depName.startsWith("google/protobuf/")) { // 标准依赖通常由protobuf库自己解决
                                        dependenciesMet = false;
                                        break;
                                    }
                                }
                            }
                            if (dependenciesMet) {
                                try {
                                    FileDescriptor fd = FileDescriptor.buildFrom(proto, dependencies.toArray(new FileDescriptor[0]));
                                    currentBatchBuiltDescriptors.put(fd.getName(), fd);
                                    registerFileDescriptor(fd);
                                    processedInLastPass++;
                                } catch (DescriptorValidationException e) {
                                    logger.log(Level.WARNING, "构建 FileDescriptor " + proto.getName() + " 失败", e);
                                }
                            } else {
                                remaining.add(proto);
                            }
                        }
                        toProcess = remaining;
                    } while (processedInLastPass > 0 && !toProcess.isEmpty());

                    if (!toProcess.isEmpty()) {
                        logger.warning("无法从反射中解析所有 FileDescriptor。剩余: " +
                                toProcess.stream().map(DescriptorProtos.FileDescriptorProto::getName).collect(Collectors.joining(", ")));
                    }
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "处理反射响应的文件描述符时出错。", e);
                } finally {
                    reflectionLatch.countDown();
                }
            }
        };

        StreamObserver<ServerReflectionRequest> requestObserver = reflectionStub.serverReflectionInfo(responseObserver);
        try {
            logger.fine("向反射端点发送 ListServicesRequest。");
            requestObserver.onNext(ServerReflectionRequest.newBuilder().setListServices("").build());
            if (parameters.getServiceName() != null && !parameters.getServiceName().isEmpty()) {
                logger.fine("为服务发送 FileContainingSymbolRequest: " + parameters.getServiceName());
                requestObserver.onNext(ServerReflectionRequest.newBuilder().setFileContainingSymbol(parameters.getServiceName()).build());
            }
            requestObserver.onCompleted();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "发送反射请求失败。", e);
            requestObserver.onError(e);
            reflectionLatch.countDown();
        }
    }

    private void registerFileDescriptor(FileDescriptor fileDescriptor) {
        if (fileDescriptor == null) return;
        logger.fine("正在注册 FileDescriptor: " + fileDescriptor.getName() + "，包名: " + fileDescriptor.getPackage());
        fileDescriptorMapByName.put(fileDescriptor.getName(), fileDescriptor);

        for (ServiceDescriptor service : fileDescriptor.getServices()) {
            logger.fine("正在注册服务: " + service.getFullName());
            serviceDescriptorCache.put(service.getFullName(), service);
        }
        for (Descriptor messageType : fileDescriptor.getMessageTypes()) {
            if (StandardTypeDescriptorRegistry.getInstance().getDescriptor(messageType.getFullName()) == null) {
                logger.fine("正在缓存消息类型 (实例特定): " + messageType.getFullName());
                messageDescriptorCache.put(messageType.getFullName(), messageType);
            }
            registerNestedMessageTypes(messageType);
        }
    }

    private void registerNestedMessageTypes(Descriptor parentMessageDescriptor) {
        for (Descriptor nestedType : parentMessageDescriptor.getNestedTypes()) {
            if (StandardTypeDescriptorRegistry.getInstance().getDescriptor(nestedType.getFullName()) == null) {
                logger.fine("正在缓存嵌套消息类型 (实例特定): " + nestedType.getFullName());
                messageDescriptorCache.put(nestedType.getFullName(), nestedType);
            }
            registerNestedMessageTypes(nestedType);
        }
    }

    private Descriptor findMessageTypeDescriptor(String messageTypeName) {
        Descriptor descriptor = messageDescriptorCache.get(messageTypeName);
        if (descriptor != null) {
            return descriptor;
        }
        descriptor = StandardTypeDescriptorRegistry.getInstance().getDescriptor(messageTypeName);
        if (descriptor != null) {
            return descriptor;
        }
        for (FileDescriptor fd : fileDescriptorMapByName.values()) {
            descriptor = fd.findMessageTypeByName(getSimpleName(messageTypeName, fd.getPackage()));
            if (descriptor != null && descriptor.getFullName().equals(messageTypeName)) {
                messageDescriptorCache.put(messageTypeName, descriptor);
                return descriptor;
            }
        }
        logger.warning("未找到消息类型 '" + messageTypeName + "' 的描述符。");
        return null;
    }

    private String getSimpleName(String fullName, String packageName) {
        if (packageName != null && !packageName.isEmpty() && fullName.startsWith(packageName + ".")) {
            return fullName.substring(packageName.length() + 1);
        }
        return fullName;
    }

    public DynamicMessage callMethod(String serviceName, String methodName, DynamicMessage request) {
        ServiceDescriptor serviceDescriptor = serviceDescriptorCache.get(serviceName);
        if (serviceDescriptor == null) {
            for(FileDescriptor fd : fileDescriptorMapByName.values()){
                if(!fd.getPackage().isEmpty()){
                    ServiceDescriptor sd = serviceDescriptorCache.get(fd.getPackage() + "." + serviceName);
                    if(sd != null) {
                        serviceDescriptor = sd;
                        serviceName = sd.getFullName();
                        break;
                    }
                }
            }
            if (serviceDescriptor == null) {
                throw new IllegalArgumentException("在任何已加载的包中均未通过完整名称或简单名称找到服务: " + serviceName);
            }
        }

        // 使用 Protobuf 的 MethodDescriptor
        com.google.protobuf.Descriptors.MethodDescriptor protoMethodDescriptor = serviceDescriptor.findMethodByName(methodName);
        if (protoMethodDescriptor == null) {
            throw new IllegalArgumentException("在服务 '" + serviceName + "' 中未找到方法 '" + methodName + "'。可用方法: " +
                    serviceDescriptor.getMethods().stream().map(com.google.protobuf.Descriptors.MethodDescriptor::getName).collect(Collectors.joining(", ")));
        }

        if (protoMethodDescriptor.isClientStreaming() || protoMethodDescriptor.isServerStreaming()) {
            throw new UnsupportedOperationException("此简单 callMethod 不支持流式 RPC。服务: " + serviceName + ", 方法: " + methodName);
        }

        // 使用 gRPC 的 MethodDescriptor
        io.grpc.MethodDescriptor<DynamicMessage, DynamicMessage> grpcMethodDescriptor =
                io.grpc.MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
                        .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                        // *** 确保调用的是 io.grpc.MethodDescriptor.generateFullMethodName ***
                        .setFullMethodName(io.grpc.MethodDescriptor.generateFullMethodName(serviceName, methodName))
                        .setRequestMarshaller(new DynamicMessageMarshaller(protoMethodDescriptor.getInputType()))
                        .setResponseMarshaller(new DynamicMessageMarshaller(protoMethodDescriptor.getOutputType()))
                        .build();

        CallOptions callOptions = CallOptions.DEFAULT;
        if (parameters.getTimeout() > 0 && parameters.getTimeoutUnit() != null) {
            callOptions = callOptions.withDeadlineAfter(parameters.getTimeout(), parameters.getTimeoutUnit());
        }

        logger.info("正在调用方法: " + grpcMethodDescriptor.getFullMethodName());
        if (logger.isLoggable(Level.FINE)) {
            try {
                logger.fine("请求 JSON: " + jsonPrinter.print(request));
            } catch (InvalidProtocolBufferException e) {
                logger.log(Level.WARNING, "将请求打印为 JSON 以进行日志记录失败", e);
            }
        }
        try {
            DynamicMessage response = ClientCalls.blockingUnaryCall(channel.newCall(grpcMethodDescriptor, callOptions), request);
            if (logger.isLoggable(Level.FINE)) {
                try {
                    logger.fine("响应 JSON: " + jsonPrinter.print(response));
                } catch (InvalidProtocolBufferException e) {
                    logger.log(Level.WARNING, "将响应打印为 JSON 以进行日志记录失败", e);
                }
            }
            return response;
        } catch (StatusRuntimeException e) {
            logger.log(Level.SEVERE, "gRPC 调用失败，状态: " + e.getStatus(), e);
            throw e;
        }
    }

    public DynamicMessage createMessage(String messageTypeName, Map<String, Object> fields) {
        Descriptor descriptor = findMessageTypeDescriptor(messageTypeName);
        if (descriptor == null) {
            throw new IllegalArgumentException("未找到消息类型描述符: " + messageTypeName);
        }
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        try {
            for (Map.Entry<String, Object> entry : fields.entrySet()) {
                Descriptors.FieldDescriptor fieldDesc = descriptor.findFieldByName(entry.getKey());
                if (fieldDesc == null) {
                    throw new IllegalArgumentException("在消息类型 '" + messageTypeName + "' 中未找到字段 '" + entry.getKey() + "'。可用字段: " + descriptor.getFields().stream().map(Descriptors.FieldDescriptor::getName).collect(Collectors.joining(", ")));
                }
                setField(builder, fieldDesc, entry.getValue());
            }
            return builder.build();
        } catch (Exception e) {
            throw new IllegalArgumentException("创建消息 " + messageTypeName + " (字段: " + fields + ") 时出错", e);
        }
    }

    private void setField(DynamicMessage.Builder builder, Descriptors.FieldDescriptor fieldDescriptor, Object value) {
        if (value == null) {
            return;
        }
        if (fieldDescriptor.isRepeated()) {
            if (!(value instanceof Iterable)) {
                throw new IllegalArgumentException("重复字段 '" + fieldDescriptor.getName() + "' 需要 Iterable 类型的值。得到: " + value.getClass().getName());
            }
            for (Object item : (Iterable<?>) value) {
                builder.addRepeatedField(fieldDescriptor, convertValue(fieldDescriptor, item));
            }
        } else {
            builder.setField(fieldDescriptor, convertValue(fieldDescriptor, value));
        }
    }

    @SuppressWarnings("unchecked")
    private Object convertValue(Descriptors.FieldDescriptor fieldDescriptor, Object value) {
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
                throw new IllegalArgumentException("消息字段 '" + fieldDescriptor.getName() + "' 需要 DynamicMessage 或 Map 类型的值。得到: " + value.getClass().getName());
            case ENUM:
                if (value instanceof Number) {
                    Descriptors.EnumValueDescriptor enumValue = fieldDescriptor.getEnumType().findValueByNumber(((Number) value).intValue());
                    if (enumValue == null) {
                        throw new IllegalArgumentException("枚举类型 " + fieldDescriptor.getEnumType().getFullName() + " 的无效数字 " + value);
                    }
                    return enumValue;
                }
                if (value instanceof String) {
                    Descriptors.EnumValueDescriptor enumValue = fieldDescriptor.getEnumType().findValueByName((String) value);
                    if (enumValue == null) {
                        throw new IllegalArgumentException("枚举类型 " + fieldDescriptor.getEnumType().getFullName() + " 的无效名称 '" + value + "'");
                    }
                    return enumValue;
                }
                if (value instanceof Descriptors.EnumValueDescriptor) {
                    return value;
                }
                throw new IllegalArgumentException("枚举字段 '" + fieldDescriptor.getName() + "' 需要 Number (int)、String 名称或 EnumValueDescriptor。得到: " + value.getClass().getName());
            case BYTES:
                if (value instanceof byte[]) {
                    return ByteString.copyFrom((byte[]) value);
                }
                if (value instanceof ByteString) {
                    return value;
                }
                throw new IllegalArgumentException("Bytes 字段 '" + fieldDescriptor.getName() + "' 需要 byte[] 或 ByteString。得到: " + value.getClass().getName());
            default:
                return value;
        }
    }

    public String messageToJson(DynamicMessage message) throws InvalidProtocolBufferException {
        return jsonPrinter.print(message);
    }

    public DynamicMessage jsonToMessage(String json, String messageTypeName) throws InvalidProtocolBufferException {
        Descriptor descriptor = findMessageTypeDescriptor(messageTypeName);
        if (descriptor == null) {
            throw new IllegalArgumentException("未找到消息类型描述符: " + messageTypeName);
        }
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        jsonParser.merge(json, builder);
        return builder.build();
    }

    public void close() {
        logger.info("GrpcDynamicClient 实例已关闭 (通道生命周期由池管理)。");
    }

    private static class DynamicMessageMarshaller implements io.grpc.MethodDescriptor.Marshaller<DynamicMessage> { // *** 修正此处的 Marshaller ***
        private final Descriptor messageDescriptor;

        public DynamicMessageMarshaller(Descriptor messageDescriptor) {
            if (messageDescriptor == null) {
                throw new IllegalArgumentException("DynamicMessageMarshaller 的消息描述符不能为空");
            }
            this.messageDescriptor = messageDescriptor;
        }

        @Override
        public InputStream stream(DynamicMessage value) {
            return value.toByteString().newInput();
        }

        @Override
        public DynamicMessage parse(InputStream stream) {
            try {
                // DynamicMessage.parseFrom 声明会抛出 InvalidProtocolBufferException (它是 IOException 的子类)
                // InputStream 上的操作本身也可能抛出其他 IOException
                return DynamicMessage.parseFrom(messageDescriptor, stream);
            } catch (IOException e) { // 捕获所有 IOException，包括 InvalidProtocolBufferException
                String logMessage;
                String statusDescription;
                if (e instanceof InvalidProtocolBufferException) {
                    logMessage = "协议缓冲区无效，从流解析类型为: " + messageDescriptor.getFullName() + " 的 DynamicMessage 失败";
                    statusDescription = "无效的协议缓冲区: " + e.getMessage();
                } else {
                    logMessage = "读取流时发生I/O错误，从流解析类型为: " + messageDescriptor.getFullName() + " 的 DynamicMessage 失败";
                    statusDescription = "读取流时发生I/O错误: " + e.getMessage();
                }
                logger.log(Level.SEVERE, logMessage, e);
                throw new StatusRuntimeException(io.grpc.Status.INTERNAL
                        .withDescription(statusDescription)
                        .withCause(e));
            }
            // 可选: 捕获其他潜在的运行时异常，尽管 protobuf 的 parseFrom 主要关注 IOException
            // catch (RuntimeException e) {
            //     logger.log(Level.SEVERE, "解析DynamicMessage时发生意外运行时错误，类型: " + messageDescriptor.getFullName(), e);
            //     throw new StatusRuntimeException(io.grpc.Status.INTERNAL
            //             .withDescription("解析消息时发生意外运行时错误: " + e.getMessage())
            //             .withCause(e));
            // }
        }
    }
}