package org.demo;

import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import lombok.Getter;

import javax.net.ssl.SSLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GrpcChannelPool {
    private static final Logger logger = Logger.getLogger(GrpcChannelPool.class.getName());
    private static final long IDLE_TIMEOUT_SECONDS = 300;  // 通道空闲超时时间（秒），可按需调整

    private static volatile GrpcChannelPool instance;
    private final Map<String, ChannelWrapper> channelMap; // 存储通道的映射，键为目标地址标识
    private final ScheduledExecutorService scheduler;     // 用于执行定时任务（如清理空闲通道）

    private GrpcChannelPool() {
        channelMap = new ConcurrentHashMap<>();
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "GrpcChannelPool-Scheduler");
            t.setDaemon(true);
            return t;
        });
        startIdleChannelCleanup();
    }

    public static GrpcChannelPool getInstance() {
        if (instance == null) {
            synchronized (GrpcChannelPool.class) {
                if (instance == null) {
                    instance = new GrpcChannelPool();
                }
            }
        }
        return instance;
    }

    public ManagedChannel getChannel(GrpcParameters parameters) throws SSLException {
        String key = generateKey(parameters);
        ChannelWrapper existingWrapper = channelMap.get(key);

        ManagedChannel channel = null;
        if (existingWrapper != null) {
            channel = existingWrapper.getChannel();
        }

        if (channel != null && !channel.isShutdown() && !channel.isTerminated()) {
            existingWrapper.updateLastAccessTime();
            logger.log(Level.FINE, "Reusing existing channel for key: {0}", key);
            return channel;
        }

        // 如果通道不存在、已关闭或已终止，则创建新通道
        // 使用 key.intern() 作为锁对象，确保对同一key的通道创建操作是同步的
        synchronized (key.intern()) {
            // 再次检查，因为在等待锁期间其他线程可能已经创建/更新了通道
            existingWrapper = channelMap.get(key);
            if (existingWrapper != null) {
                channel = existingWrapper.getChannel();
            }

            if (channel != null && !channel.isShutdown() && !channel.isTerminated()) {
                existingWrapper.updateLastAccessTime();
                logger.log(Level.FINE, "Reusing existing channel (found after lock) for key: {0}", key);
                return channel;
            }

            // 如果旧的包装器存在且其通道也存在（但无效），则尝试关闭旧通道
            if (existingWrapper != null && existingWrapper.getChannel() != null) {
                logger.log(Level.INFO, "Replacing existing unusable channel for key: {0}. Shutting down old one.", key);
                try {
                    existingWrapper.getChannel().shutdown();
                    // 短暂等待或不等待，以避免 getChannel 阻塞过久
                    // if (!existingWrapper.getChannel().awaitTermination(100, TimeUnit.MILLISECONDS)) {
                    //    existingWrapper.getChannel().shutdownNow();
                    // }
                } catch (Exception e) {
                    logger.log(Level.WARNING, "Exception shutting down old channel for key " + key + " during replacement", e);
                }
            }

            logger.log(Level.INFO, "Creating new channel for key: {0}", key);
            ManagedChannel newChannel = createChannel(parameters); // createChannel 可能抛出 SSLException
            ChannelWrapper newWrapper = new ChannelWrapper(newChannel);
            channelMap.put(key, newWrapper);
            return newChannel;
        }
    }

    private ManagedChannel createChannel(GrpcParameters parameters) throws SSLException {
        NettyChannelBuilder builder = NettyChannelBuilder.forAddress(parameters.getHost(), parameters.getPort());

        if (parameters.isUseTls()) {
            //TODO 使用更完善的SSLContext
            builder.sslContext(GrpcSslContexts.forClient().build());
            builder.useTransportSecurity();
        } else {
            builder.usePlaintext();
        }
        logger.log(Level.INFO, "Created new gRPC channel for {0}:{1} (TLS: {2})",
                new Object[]{parameters.getHost(), parameters.getPort(), parameters.isUseTls()});
        return builder.build();
    }

    String generateKey(GrpcParameters parameters) { // 包内可见，方便测试
        return parameters.getHost() + ":" + parameters.getPort() + ":" + parameters.isUseTls();
    }

    private void startIdleChannelCleanup() {
        scheduler.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            logger.log(Level.FINE, "Running idle channel cleanup task...");
            channelMap.forEach((key, wrapper) -> {
                ManagedChannel ch = wrapper.getChannel();
                if (ch != null && !ch.isShutdown() && !ch.isTerminated()) {
                    if ((now - wrapper.getLastAccessTime()) > TimeUnit.SECONDS.toMillis(IDLE_TIMEOUT_SECONDS)) {
                        logger.log(Level.INFO, "Shutting down idle channel for key: {0}. Idle time: {1}s",
                                new Object[]{key, (now - wrapper.getLastAccessTime()) / 1000});
                        try {
                            ch.shutdown();
                            if (!ch.awaitTermination(5, TimeUnit.SECONDS)) {
                                logger.log(Level.WARNING, "Idle channel (key {0}) did not terminate in 5s, calling shutdownNow().", key);
                                ch.shutdownNow();
                                if (!ch.awaitTermination(5, TimeUnit.SECONDS)) {
                                    logger.log(Level.SEVERE, "Idle channel (key {0}) failed to terminate even after shutdownNow().", key);
                                }
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            logger.log(Level.WARNING, "Channel shutdown interrupted during idle cleanup for key {0}: {1}",
                                    new Object[]{key, e.getMessage()});
                            if (ch != null) ch.shutdownNow();
                        } finally {
                            // 无论关闭是否成功，都从映射中移除，以便下次获取时创建新的
                            // 使用 remove(key, value)确保我们移除的是预期的 wrapper
                            channelMap.remove(key, wrapper);
                            logger.log(Level.INFO, "Idle channel entry removed for key: {0}", key);
                        }
                    }
                } else if (ch == null || ch.isShutdown() || ch.isTerminated()) {
                    // 如果通道已经是null或已关闭（可能由其他原因导致），也从map中移除其包装器
                    logger.log(Level.FINE, "Removing stale wrapper for key {0} (channel already null/shutdown/terminated).", key);
                    channelMap.remove(key, wrapper);
                }
            });
        }, IDLE_TIMEOUT_SECONDS, IDLE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        logger.info("Idle channel cleanup task scheduled (simplified version).");
    }

    public void shutdown() {
        logger.info("Shutting down GrpcChannelPool (simplified version)...");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.severe("Scheduler did not terminate.");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
            logger.warning("Scheduler shutdown interrupted.");
        }

        logger.info("Shutting down all channels in the map...");
        channelMap.forEach((key, wrapper) -> {
            ManagedChannel ch = wrapper.getChannel();
            if (ch != null && !ch.isShutdown()) {
                try {
                    ch.shutdown();
                    if (!ch.awaitTermination(5, TimeUnit.SECONDS)) {
                        logger.log(Level.WARNING, "Channel for key {0} did not terminate in 5s during pool shutdown, calling shutdownNow().", key);
                        ch.shutdownNow();
                        if (!ch.awaitTermination(5, TimeUnit.SECONDS)) {
                            logger.log(Level.SEVERE, "Channel for key {0} failed to terminate even after shutdownNow() during pool shutdown.", key);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.log(Level.WARNING, "Channel shutdown interrupted for key {0} during pool shutdown: {1}",
                            new Object[]{key, e.getMessage()});
                    if (ch != null) ch.shutdownNow();
                } catch (Exception ex) {
                    logger.log(Level.SEVERE, "Exception during channel shutdown for key " + key, ex);
                    if (ch != null) ch.shutdownNow();
                }
            }
        });

        channelMap.clear();
        logger.info("GrpcChannelPool has been shut down.");
    }

    // 用于测试的访问器方法
    Map<String, ChannelWrapper> getChannelMap() {
        return channelMap;
    }

    // ChannelWrapper 静态内部类
    @Getter
    static class ChannelWrapper {
        private final ManagedChannel channel; // 可以设为 final 如果构造后不再改变
        private volatile long lastAccessTime;

        public ChannelWrapper(ManagedChannel channel) {
            this.channel = channel;
            this.lastAccessTime = System.currentTimeMillis();
        }

        public void updateLastAccessTime() {
            this.lastAccessTime = System.currentTimeMillis();
        }
    }
}