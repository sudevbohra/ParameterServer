package org.petuum.ps.common.msg;

import org.petuum.ps.common.network.Msg;

import java.nio.ByteBuffer;

/**
 * Created by aqiao on 3/27/15.
 */
public class CreateTableReplyMsg extends Msg {

    private static final int TABLE_ID_OFFSET = 0;

    private static final int DATA_SIZE = TABLE_ID_OFFSET + INT_LENGTH;

    private ByteBuffer data;

    public CreateTableReplyMsg() {
        super(MsgType.CREATE_TABLE_REPLY);
        data = ByteBuffer.allocate(DATA_SIZE);
        addPayload(data);
    }

    private CreateTableReplyMsg(Msg msg) {
        super(msg);
        data = getPayload(0);
    }

    public static CreateTableReplyMsg wrap(Msg msg) {
        return new CreateTableReplyMsg(msg);
    }

    public int getTableId() {
        return data.getInt(TABLE_ID_OFFSET);
    }

    public void setTableId(int id) {
        data.putInt(TABLE_ID_OFFSET, id);
    }
}