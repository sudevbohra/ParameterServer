package org.petuum.ps.common.network;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
/**
 * Created by yihuaf on 2/23/15.
 */
public class NettyCommBusInitializer extends ChannelInitializer<SocketChannel> {

    protected final NettyCommBus commBus;

    public NettyCommBusInitializer(NettyCommBus commBus) {
        this.commBus = commBus;
    }

    @Override
    protected void initChannel(SocketChannel channel) throws Exception {
        ChannelPipeline pipeline = channel.pipeline();

        pipeline.addLast(new NettyCommBusEncoder());
        pipeline.addLast(new NettyCommBusDecoder());

        pipeline.addLast(new NettyCommBusInboundHandler(this.commBus));
    }

}