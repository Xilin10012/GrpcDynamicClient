package org.demo;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.grpc.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class GrpcDynamicClientTest {
    private GrpcDynamicClient client;
    private GrpcParameters parameters;
    private static final String PACKAGE_NAME = "test";
    private Descriptors.FileDescriptor fileDescriptor;
    
    @Mock
    private ManagedChannel mockChannel;
    
    @Mock
    private ClientCall<DynamicMessage, DynamicMessage> mockCall;
    
    @Mock
    private GrpcChannelPool mockPool;
    
    @Before
    public void setUp() throws SSLException, IOException, Descriptors.DescriptorValidationException {
        MockitoAnnotations.initMocks(this);
        
        // 创建一个简单的proto文件描述符
        DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("test.proto")
                .setPackage(PACKAGE_NAME)
                .setSyntax("proto3");
        
        // 添加消息类型
        DescriptorProtos.DescriptorProto.Builder requestMessageBuilder = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("TestRequest")
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("field1")
                        .setNumber(1)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                        .build())
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("field2")
                        .setNumber(2)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                        .build());
        
        DescriptorProtos.DescriptorProto.Builder responseMessageBuilder = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("TestResponse")
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("result")
                        .setNumber(1)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                        .build());
        
        fileDescriptorProtoBuilder.addMessageType(requestMessageBuilder.build());
        fileDescriptorProtoBuilder.addMessageType(responseMessageBuilder.build());
        
        // 添加服务定义
        DescriptorProtos.ServiceDescriptorProto.Builder serviceBuilder = DescriptorProtos.ServiceDescriptorProto.newBuilder()
                .setName("TestService")
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                        .setName("testMethod")
                        .setInputType(PACKAGE_NAME + ".TestRequest")
                        .setOutputType(PACKAGE_NAME + ".TestResponse")
                        .build());
        
        fileDescriptorProtoBuilder.addService(serviceBuilder.build());
        
        // 创建FileDescriptor
        fileDescriptor = Descriptors.FileDescriptor.buildFrom(
                fileDescriptorProtoBuilder.build(),
                new Descriptors.FileDescriptor[0]
        );
        
        parameters = GrpcParameters.builder()
                .host("localhost")
                .port(50051)
                .serviceName("TestService")
                .methodName("testMethod")
                .useTls(false)
                .timeout(5)
                .timeoutUnit(TimeUnit.SECONDS)
                .build();
        
        // 使用Mockito模拟GrpcChannelPool
        when(mockPool.getChannel(any(GrpcParameters.class))).thenReturn(mockChannel);
        
        // 使用反射设置GrpcChannelPool的instance字段
        try {
            java.lang.reflect.Field instanceField = GrpcChannelPool.class.getDeclaredField("instance");
            instanceField.setAccessible(true);
            instanceField.set(null, mockPool);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set mock GrpcChannelPool instance", e);
        }
        
        // 模拟channel.newCall的行为
        when(mockChannel.newCall(any(MethodDescriptor.class), any(CallOptions.class))).thenReturn(mockCall);
        
        client = new GrpcDynamicClient(parameters);
    }
    
    @After
    public void tearDown() {
        if (client != null) {
            client.shutdown();
        }
    }
    
    @Test
    public void testConstructor() {
        assertNotNull("Client should not be null", client);
    }
    
    @Test
    public void testCreateMessage() {
        // 创建测试消息
        Map<String, Object> fields = new HashMap<>();
        fields.put("field1", "value1");
        fields.put("field2", 123);
        
        DynamicMessage message = client.createMessage(PACKAGE_NAME + ".TestRequest", fields);
        assertNotNull("Message should not be null", message);
    }
    
    @Test
    public void testMessageToJson() {
        // 创建测试消息
        Map<String, Object> fields = new HashMap<>();
        fields.put("field1", "value1");
        fields.put("field2", 123);
        
        DynamicMessage message = client.createMessage(PACKAGE_NAME + ".TestRequest", fields);
        String json = client.messageToJson(message);
        
        assertNotNull("JSON should not be null", json);
        assertTrue("JSON should contain field1", json.contains("field1"));
        assertTrue("JSON should contain value1", json.contains("value1"));
    }
    
    @Test
    public void testJsonToMessage() {
        String json = "{\"field1\": \"value1\", \"field2\": 123}";
        DynamicMessage message = client.jsonToMessage(json, PACKAGE_NAME + ".TestRequest");
        
        assertNotNull("Message should not be null", message);
    }
    
    @Test
    public void testCallMethod() {
        // 创建测试请求消息
        Map<String, Object> fields = new HashMap<>();
        fields.put("field1", "value1");
        DynamicMessage request = client.createMessage(PACKAGE_NAME + ".TestRequest", fields);
        
        // 模拟响应消息
        DynamicMessage response = DynamicMessage.newBuilder(
                fileDescriptor.findMessageTypeByName("TestResponse"))
                .setField(fileDescriptor.findMessageTypeByName("TestResponse")
                        .findFieldByName("result"), "test result")
                .build();
        
        // 模拟ClientCall的行为
        doAnswer(invocation -> {
            ClientCall.Listener<DynamicMessage> listener = invocation.getArgument(0);
            listener.onMessage(response);
            listener.onClose(Status.OK, new Metadata());
            return null;
        }).when(mockCall).start(any(ClientCall.Listener.class), any(Metadata.class));
        
        // 调用方法
        DynamicMessage result = client.callMethod("TestService", "testMethod", request);
        assertNotNull("Response should not be null", result);
        assertEquals("test result", result.getField(result.getDescriptorForType().findFieldByName("result")));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidServiceName() {
        // 测试无效的服务名
        Map<String, Object> fields = new HashMap<>();
        fields.put("field1", "value1");
        DynamicMessage request = client.createMessage(PACKAGE_NAME + ".TestRequest", fields);
        
        client.callMethod("InvalidService", "testMethod", request);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidMethodName() {
        // 测试无效的方法名
        Map<String, Object> fields = new HashMap<>();
        fields.put("field1", "value1");
        DynamicMessage request = client.createMessage(PACKAGE_NAME + ".TestRequest", fields);
        
        client.callMethod("TestService", "invalidMethod", request);
    }

} 