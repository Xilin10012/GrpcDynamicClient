package org.demo; // 或者一个更合适的公共包

import com.google.protobuf.Any;
import com.google.protobuf.BoolValue;
import com.google.protobuf.BytesValue;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Duration;
import com.google.protobuf.Empty; // 建议也加入 Empty
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.ListValue;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.google.protobuf.Value;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StandardTypeDescriptorRegistry {
    private static final Logger logger = Logger.getLogger(StandardTypeDescriptorRegistry.class.getName());
    private static volatile StandardTypeDescriptorRegistry instance;
    private final Map<String, Descriptor> standardDescriptors;

    private StandardTypeDescriptorRegistry() {
        standardDescriptors = new ConcurrentHashMap<>();
        logger.info("Initializing StandardTypeDescriptorRegistry...");
        try {
            // 注册Google标准类型
            put(Timestamp.getDescriptor());
            put(Duration.getDescriptor());
            put(Any.getDescriptor());
            put(Struct.getDescriptor());
            put(Value.getDescriptor());
            put(ListValue.getDescriptor());
            put(Empty.getDescriptor()); // 加上 Empty

            // 注册包装类型
            put(StringValue.getDescriptor());
            put(Int32Value.getDescriptor());
            put(Int64Value.getDescriptor());
            put(BoolValue.getDescriptor());
            put(DoubleValue.getDescriptor());
            put(FloatValue.getDescriptor());
            put(BytesValue.getDescriptor());
            put(UInt32Value.getDescriptor());
            put(UInt64Value.getDescriptor());

            logger.info("StandardTypeDescriptorRegistry initialized successfully.");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to initialize standard type descriptors", e);
            // 在实际应用中，如果标准类型初始化失败，可能需要抛出更严重的异常来中止应用启动
            throw new RuntimeException("Failed to initialize standard type descriptors", e);
        }
    }

    private void put(Descriptor descriptor) {
        standardDescriptors.put(descriptor.getFullName(), descriptor);
        logger.log(Level.FINE, "Registered standard type: {0}", descriptor.getFullName());
    }

    public static StandardTypeDescriptorRegistry getInstance() {
        if (instance == null) {
            synchronized (StandardTypeDescriptorRegistry.class) {
                if (instance == null) {
                    instance = new StandardTypeDescriptorRegistry();
                }
            }
        }
        return instance;
    }

    public Descriptor getDescriptor(String typeName) {
        return standardDescriptors.get(typeName);
    }

    // 可选：提供一个方法检查是否为标准类型
    public boolean isStandardType(String typeName) {
        return standardDescriptors.containsKey(typeName);
    }
}