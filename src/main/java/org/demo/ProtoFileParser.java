package org.demo;

import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.DescriptorValidationException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class ProtoFileParser {
    private static final Logger logger = Logger.getLogger(ProtoFileParser.class.getName());
    
    /**
     * 解析单个proto文件
     * @param protoFile proto文件路径
     * @return FileDescriptor
     */
    public static FileDescriptor parseProtoFile(String protoFile) throws IOException, DescriptorValidationException {
        File file = new File(protoFile);
        if (!file.exists()) {
            throw new IOException("Proto file not found: " + protoFile);
        }
        
        try (FileInputStream fis = new FileInputStream(file)) {
            FileDescriptorProto fileDescriptorProto = FileDescriptorProto.parseFrom(fis);
            return FileDescriptor.buildFrom(fileDescriptorProto, new FileDescriptor[0]);
        }
    }
    
    /**
     * 解析包含多个proto文件的描述符集
     * @param descriptorSetFile 描述符集文件路径
     * @return 解析后的FileDescriptor列表
     */
    public static List<FileDescriptor> parseDescriptorSet(String descriptorSetFile) throws IOException, DescriptorValidationException {
        File file = new File(descriptorSetFile);
        if (!file.exists()) {
            throw new IOException("Descriptor set file not found: " + descriptorSetFile);
        }
        
        try (FileInputStream fis = new FileInputStream(file)) {
            FileDescriptorSet descriptorSet = FileDescriptorSet.parseFrom(fis);
            return buildFileDescriptors(descriptorSet);
        }
    }
    
    /**
     * 构建FileDescriptor列表，处理依赖关系
     */
    private static List<FileDescriptor> buildFileDescriptors(FileDescriptorSet descriptorSet) throws DescriptorValidationException {
        Map<String, FileDescriptor> descriptorMap = new HashMap<>();
        List<FileDescriptor> result = new ArrayList<>();
        
        // 第一遍：创建所有FileDescriptor
        for (FileDescriptorProto proto : descriptorSet.getFileList()) {
            try {
                FileDescriptor descriptor = FileDescriptor.buildFrom(proto, new FileDescriptor[0]);
                descriptorMap.put(proto.getName(), descriptor);
            } catch (DescriptorValidationException e) {
                logger.warning("Failed to build descriptor for " + proto.getName() + ": " + e.getMessage());
            }
        }
        
        // 第二遍：处理依赖关系
        for (FileDescriptorProto proto : descriptorSet.getFileList()) {
            if (descriptorMap.containsKey(proto.getName())) {
                FileDescriptor descriptor = descriptorMap.get(proto.getName());
                FileDescriptor[] dependencies = new FileDescriptor[proto.getDependencyCount()];
                
                for (int i = 0; i < proto.getDependencyCount(); i++) {
                    String dependencyName = proto.getDependency(i);
                    if (!descriptorMap.containsKey(dependencyName)) {
                        throw new IllegalArgumentException("Dependency not found: " + dependencyName);
                    }
                    dependencies[i] = descriptorMap.get(dependencyName);
                }
                
                // 重新构建带有依赖的FileDescriptor
                FileDescriptor newDescriptor = FileDescriptor.buildFrom(proto, dependencies);
                descriptorMap.put(proto.getName(), newDescriptor);
                result.add(newDescriptor);
            }
        }
        
        return result;
    }
    
    /**
     * 验证proto文件的有效性
     */
    public static void validateProtoFile(String protoFile) throws IOException {
        File file = new File(protoFile);
        if (!file.exists()) {
            throw new IOException("Proto file not found: " + protoFile);
        }
        
        if (!protoFile.endsWith(".proto")) {
            throw new IOException("Invalid file extension. Expected .proto file");
        }
        
        try (FileInputStream fis = new FileInputStream(file)) {
            FileDescriptorProto.parseFrom(fis);
        } catch (Exception e) {
            throw new IOException("Invalid proto file format: " + e.getMessage());
        }
    }
} 