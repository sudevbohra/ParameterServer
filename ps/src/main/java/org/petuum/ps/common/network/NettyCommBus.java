package org.petuum.ps.common.network;

import com.google.common.base.Preconditions;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by yihuaf on 2/23/15.
 */

public class NettyCommBus implements CommBus {

    private final ThreadLocal<NettyThreadCommInfo> threadInfo = new ThreadLocal<>();
    // Note: Although the Trove map itself is not threadsafe, the blocking queue
    // is.
    // The trove map serves to store the blocking queue reference and therefore
    // is largely read only.
    public TIntObjectMap<BlockingQueue<Msg>> nettyMsgQueues;
    // Note: this stores both incoming (coming in) and connected (connection
    // established by this thread) connection channels.
    // <selfId, <DestId, Channel>>
    public TIntObjectMap<TIntObjectMap<Channel>> channelMap;
    private int eStart;
    private int eEnd;
    private int numClients;
    private int numThreads;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public NettyCommBus(int eStart, int eEnd, int numClients, int numThreads) {
        this.eStart = eStart;
        this.eEnd = eEnd;
        this.numClients = numClients;
        this.numThreads = numThreads;
        this.nettyMsgQueues = new TIntObjectHashMap<>();
        this.channelMap = new TIntObjectHashMap<TIntObjectMap<Channel>>();
        for (int i = eStart; i < eEnd; i++) {
            nettyMsgQueues.put(getNettyMsgQueueId(i), new LinkedBlockingQueue<Msg>());
            channelMap.put(i, new TIntObjectHashMap<Channel>());
        }
        this.bossGroup = new NioEventLoopGroup(1);
        this.workerGroup = new NioEventLoopGroup();
    }

    @Override
    public void close() {
        bossGroup.shutdownGracefully().syncUninterruptibly();
        workerGroup.shutdownGracefully().syncUninterruptibly();
    }

    @Override
    public void threadRegister(Config config) {
        Preconditions.checkArgument(threadInfo.get() == null);
        threadInfo.set(new NettyThreadCommInfo());
        threadInfo.get().entityId = config.entityId;
        threadInfo.get().lType = config.lType;

        if ((config.lType & K_IN_PROC) != 0) {
            // TODO: currently for this design, there is nothing to set up for
            // inproc.
        }

        if ((config.lType & K_INTER_PROC) != 0) {
            ServerBootstrap bootstrap = new ServerBootstrap();

            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new NettyCommBusInitializer(this));

            bootstrap.option(ChannelOption.SO_BACKLOG, 128);
            bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);

            bootstrap.bind(Integer.parseInt(config.networkAddr.split(":")[1]))
                    .syncUninterruptibly().channel();
        }
    }

    @Override
    public void threadDeregister() {
        threadInfo.remove();
    }

    @Override
    public void connectTo(int entityId, Msg connectMsg) {
        connectMsg.setDest(entityId);
        connectMsg.setSender(threadInfo.get().entityId);
        try {
            nettyMsgQueues.get(getNettyMsgQueueId(entityId)).put(connectMsg);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void connectTo(int entityId, String networkAddr, Msg connectMsg) {
        Preconditions.checkArgument(!isLocalEntity(entityId));
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new NettyCommBusInitializer(this));
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);

        String ip = networkAddr.split(":")[0];
        int port = Integer.parseInt(networkAddr.split(":")[1]);
        Channel ch = bootstrap.connect(ip, port).syncUninterruptibly()
                .channel();
        channelMap.get(threadInfo.get().entityId).put(entityId, ch);
        sendInterproc(entityId, connectMsg);
    }

    @Override
    public boolean send(int entityId, Msg data) {
        if (isLocalEntity(entityId)) {
            return sendInproc(entityId, data);
        } else {
            return sendInterproc(entityId, data);
        }
    }

    @Override
    public boolean sendInproc(int entityId, Msg msg) {
        msg.setDest(entityId);
        msg.setSender(threadInfo.get().entityId);
        try {
            nettyMsgQueues.get(getNettyMsgQueueId(entityId)).put(msg);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean sendInterproc(int entityId, Msg msg) {
        Channel ch = channelMap.get(threadInfo.get().entityId).get(entityId);
        if (!ch.isActive()) {
            return false;
        }

        msg.setDest(entityId);
        msg.setSender(threadInfo.get().entityId);

        return ch.writeAndFlush(msg).syncUninterruptibly().isSuccess();
    }

    @Override
    public Msg recv() {
        Msg msg = null;
        try {
            msg = nettyMsgQueues.get(
                    getNettyMsgQueueId(threadInfo.get().entityId)).take();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }

        return msg;
    }

    @Override
    public Msg recv(long timeoutMilli) {
        try {
            Msg msg = nettyMsgQueues.get(
                    getNettyMsgQueueId(threadInfo.get().entityId)).poll(
                    timeoutMilli, TimeUnit.MILLISECONDS);
            if (msg == null) {
                return null;
            }
            return msg;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Msg recvAsync() {
        Msg msg = null;
        msg = nettyMsgQueues.get(
                getNettyMsgQueueId(threadInfo.get().entityId)).poll();
        if (msg == null) {
            return null;
        }
        return msg;
    }

    @Override
    public boolean isLocalEntity(int entityId) {
        return (eStart <= entityId) && (entityId <= eEnd);
    }

    public int getNettyMsgQueueId(int entityId) {
        return entityId - eStart;
    }

}
