package org.petuum.ps.common.msg;

import org.petuum.ps.common.network.Msg;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class ServerRowRequestReplyMsg extends Msg {

    private static final int TABLE_ID_OFFSET = 0;
    private static final int ROW_ID_OFFSET = TABLE_ID_OFFSET + INT_LENGTH;
    private static final int CLOCK_OFFSET = ROW_ID_OFFSET + INT_LENGTH;
    private static final int VERSION_OFFSET = CLOCK_OFFSET + INT_LENGTH;
    private static final int ROW_SIZE_OFFSET = VERSION_OFFSET + INT_LENGTH;

    private static final int DATA_SIZE = ROW_SIZE_OFFSET + INT_LENGTH;

    private ByteBuffer data;

    public ServerRowRequestReplyMsg() {
        super(MsgType.SERVER_ROW_REQUEST_REPLY);
        data = ByteBuffer.allocate(DATA_SIZE);
        addPayload(data);
        addPayload(null);
    }

    private ServerRowRequestReplyMsg(Msg msg) {
        super(msg);
        data = getPayload(0);
    }

    public static ServerRowRequestReplyMsg wrap(Msg msg) {
        return new ServerRowRequestReplyMsg(msg);
    }

    public int getTableId() {
        return data.getInt(TABLE_ID_OFFSET);
    }

    public void setTableId(int id) {
        data.putInt(TABLE_ID_OFFSET, id);
    }

    public int getRowId() {
        return data.getInt(ROW_ID_OFFSET);
    }

    public void setRowId(int id) {
        data.putInt(ROW_ID_OFFSET, id);
    }

    public int getClock() {
        return data.getInt(CLOCK_OFFSET);
    }

    public void setClock(int clock) {
        data.putInt(CLOCK_OFFSET, clock);
    }

    public int getVersion() {
        return data.getInt(VERSION_OFFSET);
    }

    public void setVersion(int version) {
        data.putInt(VERSION_OFFSET, version);
    }

    public int getRowSize() {
        return data.getInt(ROW_SIZE_OFFSET);
    }

    public void setRowSize(int rowSize) {
        data.putInt(ROW_SIZE_OFFSET, rowSize);
    }

    public ByteBuffer getRowData() {
        return getPayload(1);
    }

    public void setRowData(ByteBuffer rowData) {
        setPayload(1, rowData);
    }
}