package org.demo;

import com.google.protobuf.Descriptors.FileDescriptor;
import lombok.Data;
import lombok.Builder;

import java.util.concurrent.TimeUnit;

@Data
@Builder
public class GrpcParameters {
    // 基础连接参数
    private String host;
    private int port;
    private String serviceName;
    private String methodName;
    
    // 连接配置
    private boolean useTls;
    private long timeout;
    private TimeUnit timeoutUnit;
    
//    // TLS配置
//    private String caCertPath;        // CA证书路径
//    private String clientCertPath;    // 客户端证书路径
//    private String clientKeyPath;     // 客户端私钥路径
//    private String trustStorePath;    // 信任库路径
//    private String trustStorePassword; // 信任库密码
//    private String keyStorePath;      // 密钥库路径
//    private String keyStorePassword;  // 密钥库密码
    
    // 服务发现配置
    private boolean useReflection;
    private String protoFilePath;     // proto文件路径

    // 请求参数
    private String requestJson;
    
    // 重试配置
    private int maxRetries;
    private long retryDelay;
    private TimeUnit retryDelayUnit;
} 