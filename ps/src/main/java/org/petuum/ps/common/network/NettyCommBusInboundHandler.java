package org.petuum.ps.common.network;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
/**
 * Created by yihuaf on 2/23/15.
 */
@Sharable
public class NettyCommBusInboundHandler extends ChannelInboundHandlerAdapter {
    protected NettyCommBus commBus;

    public NettyCommBusInboundHandler(NettyCommBus commBus) {
        super();
        this.commBus = commBus;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object obj)
            throws Exception {

        Msg msg = (Msg) obj;

        int dest = msg.getDest();
        int sender = msg.getSender();

        BlockingQueue<Msg> queue = commBus.nettyMsgQueues
                .get(commBus.getNettyMsgQueueId(dest));
        
        if (commBus.channelMap.get(dest).get(sender) == null) {
            commBus.channelMap.get(dest).put(sender, ctx.channel());
        }
        
        queue.put(msg);

    }

}