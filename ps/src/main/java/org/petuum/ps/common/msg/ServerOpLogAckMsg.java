package org.petuum.ps.common.msg;

import org.petuum.ps.common.network.Msg;

import java.nio.ByteBuffer;

public class ServerOpLogAckMsg extends Msg {

    private static final int ACK_VERSION_OFFSET = 0;

    private static final int DATA_SIZE = ACK_VERSION_OFFSET + INT_LENGTH;

    private ByteBuffer data;

    public ServerOpLogAckMsg() {
        super(MsgType.SERVER_OP_LOG_ACK);
        data = ByteBuffer.allocate(DATA_SIZE);
        addPayload(data);
    }

    private ServerOpLogAckMsg(Msg msg) {
        super(msg);
        data = getPayload(0);
    }

    public static ServerOpLogAckMsg wrap(Msg msg) {
        return new ServerOpLogAckMsg(msg);
    }

    public int getAckVersion() {
        return data.getInt(ACK_VERSION_OFFSET);
    }

    public void setAckVersion(int version) {
        data.putInt(ACK_VERSION_OFFSET, version);
    }
}
