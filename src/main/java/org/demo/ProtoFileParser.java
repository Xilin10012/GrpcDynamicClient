package org.demo;

import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.DescriptorValidationException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
} 