package org.demo;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * gRPC task parameters
 */
@Data
public class GrpcParameters {

    /**
     * gRPC server host
     */
    private String host;

    /**
     * gRPC server port
     */
    private int port;

    /**
     * Full service name (e.g., "package.ServiceName")
     */
    private String serviceName;

    /**
     * Method name to call
     */
    private String methodName;

    /**
     * Request parameters in JSON format
     */
    private String requestJson;

    /**
     * Timeout in seconds
     */
    private int timeoutSeconds = 30;

    /**
     * Maximum number of retries
     */
    private int retryTimes = 3;

    /**
     * Retry interval in seconds
     */
    private int retryIntervalSeconds = 5;

    /**
     * Whether to use TLS/SSL
     */
    private boolean useTls = false;

    /**
     * CA certificate path
     */
    private String caCertPath;

    /**
     * Client certificate path
     */
    private String clientCertPath;

    /**
     * Client private key path
     */
    private String clientKeyPath;

    /**
     * Whether to use reflection for service discovery
     */
    private boolean useReflection = false;

    /**
     * Output variables mapping (JSON path -> variable name)
     */
    private List<Property> outputVariables;

    @Override
    public boolean checkParameters() {
        boolean basicParamsValid = StringUtils.isNotEmpty(host) 
            && port > 0 
            && StringUtils.isNotEmpty(serviceName)
            && StringUtils.isNotEmpty(methodName)
            && StringUtils.isNotEmpty(requestJson);

        boolean tlsParamsValid = !useTls || (StringUtils.isNotEmpty(caCertPath) 
            && StringUtils.isNotEmpty(clientCertPath) 
            && StringUtils.isNotEmpty(clientKeyPath));

        return basicParamsValid && tlsParamsValid;
    }
} 