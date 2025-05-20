package org.demo;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;

import javax.net.ssl.SSLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class GrpcChannelPool {
    private static final Logger logger = Logger.getLogger(GrpcChannelPool.class.getName());
    private static final int MAX_CHANNELS_PER_TARGET = 5; // 每个目标地址的最大通道数
    private static final long IDLE_TIMEOUT_SECONDS = 5;  // 通道空闲超时时间（秒）

    private static GrpcChannelPool instance;
    private final Map<String, ChannelWrapper> channelMap; // 存储通道的映射，键为目标地址标识
    private final Map<String, Integer> channelCountMap;   // 记录每个目标地址当前的活动通道数
    private final ScheduledExecutorService scheduler;     // 用于执行定时任务（如清理空闲通道）

    private GrpcChannelPool() {
        channelMap = new ConcurrentHashMap<>();
        channelCountMap = new ConcurrentHashMap<>();
        // 使用单线程的 ScheduledExecutorService
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "GrpcChannelPool-Scheduler");
            t.setDaemon(true); // 设置为守护线程，以便JVM退出时它能自动结束
            return t;
        });
        startIdleChannelCleanup();
    }

    public static synchronized GrpcChannelPool getInstance() {
        if (instance == null) {
            instance = new GrpcChannelPool();
        }
        return instance;
    }

    public synchronized ManagedChannel getChannel(GrpcParameters parameters) throws SSLException {
        String key = generateKey(parameters);
        ChannelWrapper wrapper = channelMap.get(key);

        // 1. 尝试复用现有的、有效的通道
        if (wrapper != null && wrapper.channel != null && !wrapper.channel.isShutdown() && !wrapper.channel.isTerminated()) {
            wrapper.lastAccessTime = System.currentTimeMillis();
            logger.fine("Reusing existing channel for key: " + key);
            return wrapper.channel;
        }

        // 2. 如果没有有效的现有通道 (wrapper为null, 或wrapper.channel为null/shutdown/terminated)
        //    检查是否允许为该目标创建新通道
        //    注意：如果 wrapper.channel 是因为空闲被关闭的，idleChannelCleanup 方法应该已经减少了 channelCountMap 中的计数
        int currentCount = channelCountMap.getOrDefault(key, 0);

        if (currentCount < MAX_CHANNELS_PER_TARGET) {
            logger.fine("Creating new channel for key: " + key + ". Current count before creation: " + currentCount);
            ManagedChannel newChannel = createChannel(parameters);
            ChannelWrapper newWrapper = new ChannelWrapper(newChannel);
            channelMap.put(key, newWrapper); // 存储新的或替换的通道包装器

            // 增加此键的计数，因为我们为其配置了一个新的活动通道。
            // 这假设如果一个旧通道被清理，它的计数已经被减少了。
            channelCountMap.merge(key, 1, Integer::sum);
            return newChannel;
        } else {
            // currentCount >= MAX_CHANNELS_PER_TARGET
            // 已达到此键的限制，并且 'wrapper' 中的通道（如果有的话）不可用。
            logger.warning("Max channels (" + MAX_CHANNELS_PER_TARGET + ") for key " + key +
                    " reached, and the existing channel (if any) is unusable. Waiting and retrying.");
            try {
                // 这里使用了短暂的阻塞睡眠。在高性能应用中，应考虑非阻塞策略或带有超时的有界队列。
                Thread.sleep(100);
                return getChannel(parameters); // 递归调用；确保它最终能解决或抛出异常
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for channel", e);
            }
        }
    }

    private ManagedChannel createChannel(GrpcParameters parameters) throws SSLException {
        NettyChannelBuilder builder = NettyChannelBuilder.forAddress(parameters.getHost(), parameters.getPort());

        if (parameters.isUseTls()) {
            // 如果使用真实TLS，需要确保SSLContext已正确配置。
            // 为简单起见，这里假设 NettyChannelBuilder 会处理必要的TLS设置，
            // 或者如果 GrpcParameters 中有 SslContext，则会使用它。
            builder.useTransportSecurity();
        } else {
            builder.usePlaintext();
        }
        // 可以添加更多配置，如 keepAliveTime, keepAliveTimeout, idleTimeout (gRPC层)等
        // builder.keepAliveTime(10, TimeUnit.SECONDS)
        //        .keepAliveTimeout(5, TimeUnit.SECONDS)
        //        .idleTimeout(IDLE_TIMEOUT_SECONDS * 2, TimeUnit.SECONDS); // gRPC的idleTimeout与池的idleTimeout概念不同

        return builder.build();
    }

    // 包内可见，方便测试
    String generateKey(GrpcParameters parameters) {
        return parameters.getHost() + ":" + parameters.getPort() + ":" + parameters.isUseTls();
    }

    private void startIdleChannelCleanup() {
        scheduler.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            logger.fine("Running idle channel cleanup task...");
            channelMap.forEach((key, wrapper) -> {
                synchronized (wrapper) { // 同步访问 wrapper，避免并发修改问题
                    if (wrapper.channel != null && !wrapper.channel.isShutdown() && !wrapper.channel.isTerminated()) {
                        if ((now - wrapper.lastAccessTime) > TimeUnit.SECONDS.toMillis(IDLE_TIMEOUT_SECONDS)) {
                            logger.info("Shutting down idle channel for key: " + key +
                                    ". Idle time: " + (now - wrapper.lastAccessTime) / 1000 + "s");
                            try {
                                wrapper.channel.shutdown();
                                if (!wrapper.channel.awaitTermination(5, TimeUnit.SECONDS)) {
                                    logger.warning("Idle channel for key " + key + " did not terminate in 5s, calling shutdownNow().");
                                    wrapper.channel.shutdownNow();
                                    if (!wrapper.channel.awaitTermination(5, TimeUnit.SECONDS)) {
                                        logger.severe("Idle channel for key " + key + " failed to terminate even after shutdownNow().");
                                    }
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                logger.warning("Channel shutdown interrupted during idle cleanup for key " + key + ": " + e.getMessage());
                                // 尝试强制关闭
                                wrapper.channel.shutdownNow();
                            }
                            // 标记通道已关闭，即使关闭失败，也防止后续使用
                            wrapper.channel = null;
                            channelCountMap.merge(key, -1, Integer::sum); // 减少活动通道计数
                            logger.info("Idle channel removed for key: " + key + ". New count: " + channelCountMap.getOrDefault(key, 0));
                        }
                    } else if (wrapper.channel == null) {
                        // 如果包装器中的通道已经是null（可能被其他方式关闭或从未成功创建），
                        // 且计数仍存在，这可能是一个不一致的状态，或者表示一个已清理的槽位。
                        // 考虑是否需要在这里从 channelMap 中移除这样的 wrapper，如果它的 count 也是0。
                        // channelMap.remove(key, wrapper); // 仅当 wrapper.channel is null 且 count is 0
                    }
                }
            });
            // 可选：清理 channelMap 中那些 channel 为 null 且对应 count 也为 0 的条目
            // channelMap.entrySet().removeIf(entry -> {
            //    synchronized (entry.getValue()) {
            //        return entry.getValue().channel == null && channelCountMap.getOrDefault(entry.getKey(), 0) <= 0;
            //    }
            // });
        }, IDLE_TIMEOUT_SECONDS, IDLE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        logger.info("Idle channel cleanup task scheduled.");
    }

    public void shutdown() {
        logger.info("Shutting down GrpcChannelPool...");
        // 1. 停止调度新的清理任务，并尝试优雅地关闭调度器
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

        // 2. 关闭所有池中的通道
        logger.info("Shutting down all channels in the pool...");
        channelMap.forEach((key, wrapper) -> {
            synchronized (wrapper) {
                if (wrapper.channel != null && !wrapper.channel.isShutdown()) {
                    try {
                        wrapper.channel.shutdown();
                        if (!wrapper.channel.awaitTermination(5, TimeUnit.SECONDS)) {
                            logger.warning("Channel for key " + key + " did not terminate in 5s during pool shutdown, calling shutdownNow().");
                            wrapper.channel.shutdownNow();
                            if (!wrapper.channel.awaitTermination(5, TimeUnit.SECONDS)) {
                                logger.severe("Channel for key " + key + " failed to terminate even after shutdownNow() during pool shutdown.");
                            }
                        }
                        logger.fine("Successfully shutdown channel for key: " + key);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.warning("Channel shutdown interrupted for key " + key + " during pool shutdown: " + e.getMessage());
                        // 确保即使在中断时也尝试强制关闭
                        if(wrapper.channel != null) { // 再次检查，因为中断可能发生在任何地方
                            wrapper.channel.shutdownNow();
                        }
                    }
                }
            }
        });

        // 3. 清理映射
        channelMap.clear();
        channelCountMap.clear();
        logger.info("GrpcChannelPool has been shut down.");
    }

    // 用于测试的访问器方法
    Map<String, ChannelWrapper> getChannelMap() {
        return channelMap;
    }

    Map<String, Integer> getChannelCountMap() { // 添加此方法以便测试可以访问 channelCountMap
        return channelCountMap;
    }

    static class ChannelWrapper {
        ManagedChannel channel;
        long lastAccessTime;

        public ChannelWrapper(ManagedChannel channel) {
            this.channel = channel;
            this.lastAccessTime = System.currentTimeMillis();
        }

        // 无参构造函数，如果 Mockito 等框架需要的话
        public ChannelWrapper() {
            this.lastAccessTime = System.currentTimeMillis();
        }
    }
}