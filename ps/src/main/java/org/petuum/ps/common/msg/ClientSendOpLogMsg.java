package org.petuum.ps.common.msg;

import org.petuum.ps.common.network.Msg;

import java.nio.ByteBuffer;

/**
 * Created by aqiao on 3/27/15.
 */
public class ClientSendOpLogMsg extends Msg {

    private static final int IS_CLOCK_OFFSET = 0;
    private static final int CLIENT_ID_OFFSET = IS_CLOCK_OFFSET + INT_LENGTH;
    private static final int VERSION_OFFSET = CLIENT_ID_OFFSET + INT_LENGTH;
    private static final int BG_CLOCK_OFFSET = VERSION_OFFSET + INT_LENGTH;

    private static final int DATA_SIZE = BG_CLOCK_OFFSET + INT_LENGTH;

    private ByteBuffer data;

    public ClientSendOpLogMsg(int avaiSize) {
        super(MsgType.CLIENT_SEND_OP_LOG);
        data = ByteBuffer.allocate(DATA_SIZE);
        addPayload(data);
        if (avaiSize > 0) {
            addPayload(ByteBuffer.allocate(avaiSize));
        } else {
            addPayload(null);
        }
    }

    private ClientSendOpLogMsg(Msg msg) {
        super(msg);
        data = getPayload(0);
    }

    public static ClientSendOpLogMsg wrap(Msg msg) {
        return new ClientSendOpLogMsg(msg);
    }

    public boolean getIsClock() {
        return data.getInt(IS_CLOCK_OFFSET) != 0;
    }

    public void setIsClock(boolean isClock) {
        data.putInt(IS_CLOCK_OFFSET, isClock ? 1 : 0);
    }

    public int getClientId() {
        return data.getInt(CLIENT_ID_OFFSET);
    }

    public void setClientId(int id) {
        data.putInt(CLIENT_ID_OFFSET, id);
    }

    public int getVersion() {
        return data.getInt(VERSION_OFFSET);
    }

    public void setVersion(int version) {
        data.putInt(VERSION_OFFSET, version);
    }

    public int getBgClock() {
        return data.getInt(BG_CLOCK_OFFSET);
    }

    public void setBgClock(int bgClock) {
        data.putInt(BG_CLOCK_OFFSET, bgClock);
    }

    public ByteBuffer getData() {
        return getPayload(1);
    }

    public int getAvaiSize() {
        if (getPayload(1) != null) {
            return getPayload(1).capacity();
        } else {
            return 0;
        }
    }
}
