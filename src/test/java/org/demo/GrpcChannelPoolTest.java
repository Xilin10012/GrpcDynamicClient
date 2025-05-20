package org.demo;

import io.grpc.ManagedChannel;
// import org.junit.jupiter.api.AfterEach; // 如果使用 JUnit 5
// import org.junit.jupiter.api.BeforeEach;
// import org.junit.jupiter.api.Test;
import org.junit.After; // JUnit 4
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock; // 保留，如果其他测试或扩展需要 Mockito
import org.mockito.MockitoAnnotations;


import javax.net.ssl.SSLException;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*; // 保留，以备将来使用

public class GrpcChannelPoolTest {
    private static final long IDLE_TIMEOUT_SECONDS = 5;
    private GrpcChannelPool channelPool;
    private GrpcParameters parameters;

    // 重置单例的辅助方法
    private void resetGrpcChannelPoolSingleton() throws Exception {
        Field instanceField = GrpcChannelPool.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        instanceField.set(null, null);
    }


    @Before
    public void setUp() throws Exception {
        // 关闭 gRPC 内部的 ManagedChannelOrphanWrapper 的大量日志
        Logger.getLogger("io.grpc.internal.ManagedChannelOrphanWrapper").setLevel(Level.OFF);

        // 每次测试前重置单例，确保测试隔离性
        resetGrpcChannelPoolSingleton();
        MockitoAnnotations.initMocks(this); // 初始化 @Mock 字段
        channelPool = GrpcChannelPool.getInstance(); // 获取新的或重置后的实例

        parameters = GrpcParameters.builder()
                .host("localhost")
                .port(50051)
                .useTls(false)
                .build();
    }

    @After
    public void tearDown() {
        if (channelPool != null) {
            channelPool.shutdown(); // 连接池自己的 shutdown 方法会负责清理所有资源
        }
    }

    @Test
    public void testGetInstance() {
        GrpcChannelPool instance1 = GrpcChannelPool.getInstance();
        GrpcChannelPool instance2 = GrpcChannelPool.getInstance();
        assertSame("应该返回同一个实例", instance1, instance2);
    }

    @Test
    public void testGetChannel() throws SSLException {
        ManagedChannel channel1 = channelPool.getChannel(parameters);
        assertNotNull("通道不应为null", channel1);
        assertFalse("获取后通道不应立即关闭", channel1.isShutdown());
    }

    @Test
    public void testChannelReuse() throws SSLException {
        ManagedChannel channel1 = channelPool.getChannel(parameters);
        ManagedChannel channel2 = channelPool.getChannel(parameters);
        assertSame("对于相同的参数，应该复用同一个通道", channel1, channel2);
    }

    @Test
    public void testTlsChannel() {
        GrpcParameters tlsParameters = GrpcParameters.builder()
                .host("localhost")
                .port(50051)
                .useTls(true)
                // .caCertPath("path/to/ca.crt") // 真实TLS可能需要更多配置
                .build();
        try {
            ManagedChannel channel = channelPool.getChannel(tlsParameters);
            assertNotNull("TLS 通道不应为 null", channel);
            assertFalse("TLS 通道不应被关闭", channel.isShutdown());
        } catch (SSLException e) {
            // 如果测试环境没有完整的TLS证书，这里可能会抛出SSLException。
            // 对于某些简单的 useTransportSecurity() 调用，Netty可能会使用默认的自签名上下文，
            // 这取决于具体的gRPC和Netty版本以及操作系统配置。
            // 此处断言失败，表示我们不期望在基础TLS通道创建时出现SSLException。
            fail("基础TLS通道创建时不应出现SSLException: " + e.getMessage());
        } catch (Exception e) {
            fail("使用TLS创建通道时发生意外失败: " + e.getMessage());
        }
    }

    @Test
    public void testShutdown() throws SSLException, InterruptedException {
        // 为了测试 shutdown，我们需要一个真实的或被充分 mock 的 channel
        // setUp 中的 mockChannel 如果没有被 getChannel 使用，则需要手动放入池中
        ManagedChannel testManagedChannel = mock(ManagedChannel.class);
        when(testManagedChannel.shutdown()).thenReturn(testManagedChannel);
        when(testManagedChannel.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(testManagedChannel.isShutdown()).thenReturn(false); // 初始状态

        // 手动将 mock channel 放入连接池的 map 中以进行此特定测试
        String key = channelPool.generateKey(parameters);
        GrpcChannelPool.ChannelWrapper wrapper = new GrpcChannelPool.ChannelWrapper(testManagedChannel);
        channelPool.getChannelMap().put(key, wrapper);
        channelPool.getChannelCountMap().put(key, 1); // 模拟它已被获取


        channelPool.shutdown(); // 调用主要的 shutdown 方法

        // 验证 mock channel 的 shutdown 和 awaitTermination 是否被调用
        verify(testManagedChannel, timeout(2000).times(1)).shutdown(); // 增加超时以应对可能的延迟
        verify(testManagedChannel, timeout(2000).times(1)).awaitTermination(anyLong(), any(TimeUnit.class));

        assertTrue("关闭后 channelMap 应为空", channelPool.getChannelMap().isEmpty());
        assertTrue("关闭后 channelCountMap 应为空", channelPool.getChannelCountMap().isEmpty());
    }

    @Test
    public void testIdleChannelCleanup() throws SSLException, InterruptedException {
        ManagedChannel channel1 = channelPool.getChannel(parameters);
        assertNotNull(channel1);
        String key = channelPool.generateKey(parameters);
        assertEquals("获取通道后，计数应为1", 1, (int)channelPool.getChannelCountMap().getOrDefault(key, 0));


        // 等待时间略长于空闲超时时间，再加上一点缓冲时间确保调度器任务执行
        Thread.sleep(TimeUnit.SECONDS.toMillis(IDLE_TIMEOUT_SECONDS + 3)); // 例如5秒超时 + 3秒缓冲

        GrpcChannelPool.ChannelWrapper wrapperAfterCleanup = channelPool.getChannelMap().get(key);

        // 检查通道是否已被清理
        if (wrapperAfterCleanup != null) {
            // 在修改后的逻辑中，wrapper.channel 会被设为 null
            assertNull("空闲清理后，包装器中的通道应为null", wrapperAfterCleanup.channel);
        }
        // 计数应该被减少
        assertEquals("空闲清理后，对应键的通道计数应为0", 0, (int)channelPool.getChannelCountMap().getOrDefault(key, -1)); // 用-1作为默认值以区分0和不存在

        // 再次请求相同参数的通道，应该会创建一个新的、活动的通道
        ManagedChannel channel2 = channelPool.getChannel(parameters);
        assertNotNull(channel2);
        assertNotSame("旧通道被空闲清理后，应获取一个新的通道实例", channel1, channel2);
        assertFalse("新通道不应被关闭", channel2.isShutdown());
        assertEquals("获取新通道后，计数应恢复为1", 1, (int)channelPool.getChannelCountMap().getOrDefault(key, 0));
    }

    @Test
    public void testMaxChannelsPerTarget() throws SSLException {
        final int numTargetsToTest = 6; // 尝试为6个不同的目标获取通道
        ManagedChannel[] channels = new ManagedChannel[numTargetsToTest];

        for (int i = 0; i < numTargetsToTest; i++) {
            GrpcParameters params = GrpcParameters.builder()
                    .host("localhost")
                    .port(50051 + i) // 不同的端口意味着不同的目标键
                    .useTls(false)
                    .build();
            channels[i] = channelPool.getChannel(params);
            assertNotNull("通道 " + i + " 不应为 null", channels[i]);
        }

        // 计算获取到的唯一通道实例的数量。
        // 由于每个 GrpcParameters 对象产生唯一的键，并且对于每个新键，
        // 通道计数从0开始（小于 MAX_CHANNELS_PER_TARGET），
        // 连接池将为6个不同的目标中的每一个创建一个新通道。
        // 因此，所有6个通道都应该是唯一的。
        Set<ManagedChannel> uniqueChannelSet = new HashSet<>();
        for (int i = 0; i < numTargetsToTest; i++) {
            if (channels[i] != null) {
                uniqueChannelSet.add(channels[i]);
            }
        }

        // MAX_CHANNELS_PER_TARGET 是每个目标的限制，而不是跨不同目标的全局唯一通道限制。
        // 因此，这里预期得到 6 个唯一通道。
        assertEquals("当为" + numTargetsToTest + "个不同目标请求通道时，应有" + numTargetsToTest + "个唯一通道",
                numTargetsToTest, uniqueChannelSet.size());
    }

    @Test
    public void testDifferentTargets() throws SSLException {
        GrpcParameters params1 = GrpcParameters.builder()
                .host("localhost")
                .port(50051)
                .useTls(false)
                .build();

        GrpcParameters params2 = GrpcParameters.builder()
                .host("anotherhost") // 不同的主机
                .port(50051)
                .useTls(false)
                .build();

        GrpcParameters params3 = GrpcParameters.builder()
                .host("localhost")
                .port(50052) // 不同的端口
                .useTls(false)
                .build();

        GrpcParameters params4 = GrpcParameters.builder()
                .host("localhost")
                .port(50051)
                .useTls(true) // 不同的TLS设置
                .build();

        ManagedChannel channel1 = channelPool.getChannel(params1); // localhost:50051:false
        ManagedChannel channel2 = channelPool.getChannel(params2); // anotherhost:50051:false
        ManagedChannel channel3 = channelPool.getChannel(params3); // localhost:50052:false
        ManagedChannel channel4 = channelPool.getChannel(params4); // localhost:50051:true


        assertNotNull(channel1);
        assertNotNull(channel2);
        assertNotNull(channel3);
        assertNotNull(channel4);

        assertNotSame("不同主机的通道应该是不同的", channel1, channel2);
        assertNotSame("不同端口的通道应该是不同的", channel1, channel3);
        assertNotSame("不同TLS设置的通道应该是不同的", channel1, channel4);
    }
}