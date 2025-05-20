package org.demo;

import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.logging.Logger;

public class GrpcSslContext {
    private static final Logger logger = Logger.getLogger(GrpcSslContext.class.getName());

    public static ManagedChannel createSecureChannel(GrpcParameters parameters) throws SSLException {
        if (!parameters.isUseTls()) {
            return NettyChannelBuilder.forAddress(parameters.getHost(), parameters.getPort())
                    .usePlaintext()
                    .build();
        }

        SslContext sslContext = createSslContext(parameters);
        
        return NettyChannelBuilder.forAddress(parameters.getHost(), parameters.getPort())
                .negotiationType(NegotiationType.TLS)
                .sslContext(sslContext)
                .build();
    }

    public static SslContext createSslContext(GrpcParameters parameters) throws SSLException {
        try {
            SslContextBuilder builder = GrpcSslContexts.forClient();

            // 设置SSL提供者
            builder.sslProvider(SslProvider.OPENSSL);

            // 配置CA证书
            if (parameters.getCaCertPath() != null) {
                builder.trustManager(new File(parameters.getCaCertPath()));
            }

            // 配置信任库
            if (parameters.getTrustStorePath() != null) {
                KeyStore trustStore = KeyStore.getInstance("JKS");
                try (FileInputStream fis = new FileInputStream(parameters.getTrustStorePath())) {
                    trustStore.load(fis, parameters.getTrustStorePassword().toCharArray());
                }
                
                // 创建TrustManagerFactory
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(trustStore);
                
                // 使用TrustManagerFactory配置builder
                builder.trustManager(tmf);
            }

            // 配置客户端证书和私钥
            if (parameters.getClientCertPath() != null && parameters.getClientKeyPath() != null) {
                builder.keyManager(
                    new File(parameters.getClientCertPath()),
                    new File(parameters.getClientKeyPath())
                );
            }

            // 配置密钥库
            if (parameters.getKeyStorePath() != null) {
                KeyStore keyStore = KeyStore.getInstance("JKS");
                try (FileInputStream fis = new FileInputStream(parameters.getKeyStorePath())) {
                    keyStore.load(fis, parameters.getKeyStorePassword().toCharArray());
                }
                
                // 创建KeyManagerFactory
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(keyStore, parameters.getKeyStorePassword().toCharArray());
                
                // 使用KeyManagerFactory配置builder
                builder.keyManager(kmf);
            }

            return builder.build();
        } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException | UnrecoverableKeyException e) {
            throw new SSLException("Failed to create SSL context", e);
        }
    }

    public static void validateSslConfig(GrpcParameters parameters) {
        if (!parameters.isUseTls()) {
            return;
        }

        // 验证CA证书
        if (parameters.getCaCertPath() != null) {
            File caCertFile = new File(parameters.getCaCertPath());
            if (!caCertFile.exists()) {
                throw new IllegalArgumentException("CA certificate file not found: " + parameters.getCaCertPath());
            }
        }

        // 验证客户端证书和私钥
        if (parameters.getClientCertPath() != null || parameters.getClientKeyPath() != null) {
            if (parameters.getClientCertPath() == null || parameters.getClientKeyPath() == null) {
                throw new IllegalArgumentException("Both client certificate and key must be provided");
            }
            File clientCertFile = new File(parameters.getClientCertPath());
            File clientKeyFile = new File(parameters.getClientKeyPath());
            if (!clientCertFile.exists()) {
                throw new IllegalArgumentException("Client certificate file not found: " + parameters.getClientCertPath());
            }
            if (!clientKeyFile.exists()) {
                throw new IllegalArgumentException("Client key file not found: " + parameters.getClientKeyPath());
            }
        }

        // 验证信任库
        if (parameters.getTrustStorePath() != null) {
            File trustStoreFile = new File(parameters.getTrustStorePath());
            if (!trustStoreFile.exists()) {
                throw new IllegalArgumentException("Trust store file not found: " + parameters.getTrustStorePath());
            }
            if (parameters.getTrustStorePassword() == null) {
                throw new IllegalArgumentException("Trust store password must be provided");
            }
        }

        // 验证密钥库
        if (parameters.getKeyStorePath() != null) {
            File keyStoreFile = new File(parameters.getKeyStorePath());
            if (!keyStoreFile.exists()) {
                throw new IllegalArgumentException("Key store file not found: " + parameters.getKeyStorePath());
            }
            if (parameters.getKeyStorePassword() == null) {
                throw new IllegalArgumentException("Key store password must be provided");
            }
        }
    }
} 