package org.demo;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.grpc.ManagedChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class GrpcDynamicClientTest {
    private GrpcDynamicClient client;
    private GrpcParameters parameters;
    
    @Mock
    private ManagedChannel mockChannel;
    
    @Before
    public void setUp() throws SSLException, IOException, Descriptors.DescriptorValidationException {
        MockitoAnnotations.initMocks(this);
        
        parameters = GrpcParameters.builder()
                .host("localhost")
                .port(50051)
                .serviceName("TestService")
                .methodName("testMethod")
                .useTls(false)
                .build();
        
        // 使用Mockito模拟GrpcChannelPool
        GrpcChannelPool mockPool = mock(GrpcChannelPool.class);
        when(mockPool.getChannel(any(GrpcParameters.class))).thenReturn(mockChannel);
        
        client = new GrpcDynamicClient(parameters);
    }
    
    @After
    public void tearDown() {
        client.shutdown();
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
        
        DynamicMessage message = client.createMessage("TestMessage", fields);
        assertNotNull("Message should not be null", message);
    }
    
    @Test
    public void testMessageToJson() {
        // 创建测试消息
        Map<String, Object> fields = new HashMap<>();
        fields.put("field1", "value1");
        fields.put("field2", 123);
        
        DynamicMessage message = client.createMessage("TestMessage", fields);
        String json = client.messageToJson(message);
        
        assertNotNull("JSON should not be null", json);
        assertTrue("JSON should contain field1", json.contains("field1"));
        assertTrue("JSON should contain value1", json.contains("value1"));
    }
    
    @Test
    public void testJsonToMessage() {
        String json = "{\"field1\": \"value1\", \"field2\": 123}";
        DynamicMessage message = client.jsonToMessage(json, "TestMessage");
        
        assertNotNull("Message should not be null", message);
    }
    
    @Test
    public void testCallMethod() {
        // 创建测试请求消息
        Map<String, Object> fields = new HashMap<>();
        fields.put("field1", "value1");
        DynamicMessage request = client.createMessage("TestRequest", fields);
        
        // 调用方法
        DynamicMessage response = client.callMethod("TestService", "testMethod", request);
        assertNotNull("Response should not be null", response);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidServiceName() {
        // 测试无效的服务名
        Map<String, Object> fields = new HashMap<>();
        fields.put("field1", "value1");
        DynamicMessage request = client.createMessage("TestRequest", fields);
        
        client.callMethod("InvalidService", "testMethod", request);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidMethodName() {
        // 测试无效的方法名
        Map<String, Object> fields = new HashMap<>();
        fields.put("field1", "value1");
        DynamicMessage request = client.createMessage("TestRequest", fields);
        
        client.callMethod("TestService", "invalidMethod", request);
    }
    
    @Test
    public void testShutdown() {
        // 测试关闭客户端
        client.shutdown();
        // 验证channel是否被正确关闭
        verify(mockChannel, times(1)).shutdown();
    }
} 