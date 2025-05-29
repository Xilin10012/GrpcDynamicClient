package org.demo;

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
    
    // 服务发现配置
    private boolean useReflection;
    private String protoFilePath;     // proto文件路径

    // 请求参数
    private String requestJson;
    
    // 重试配置
    private int maxRetries;
    private long retryDelay;
    private TimeUnit retryDelayUnit;
    private String retryStatusCode;

} 