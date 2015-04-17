package org.petuum.ps.common.msg;

import org.petuum.ps.common.network.Msg;
import org.petuum.ps.config.Config;

import java.nio.ByteBuffer;

/**
 * Created by aqiao on 3/27/15.
 */
public class CreateTableMsg extends Msg {

    private static final int TABLE_ID_OFFSET = 0;
    private static final int STALENESS_OFFSET = TABLE_ID_OFFSET + INT_LENGTH;
    private static final int ROW_TYPE_OFFSET = STALENESS_OFFSET + INT_LENGTH;
    private static final int ROW_CAPACITY_OFFSET = ROW_TYPE_OFFSET + INT_LENGTH;
    private static final int OPLOG_DENSE_SERIALIZED_OFFSET = ROW_CAPACITY_OFFSET + INT_LENGTH;
    private static final int ROW_OPLOG_TYPE_OFFSET = OPLOG_DENSE_SERIALIZED_OFFSET + INT_LENGTH;
    private static final int DENSE_ROW_OPLOG_CAPACITY_OFFSET = ROW_OPLOG_TYPE_OFFSET + INT_LENGTH;

    private static final int DATA_SIZE = DENSE_ROW_OPLOG_CAPACITY_OFFSET + INT_LENGTH;

    private ByteBuffer data;

    public CreateTableMsg() {
        super(MsgType.CREATE_TABLE);
        data = ByteBuffer.allocate(DATA_SIZE);
        addPayload(data);
        addPayload(null);
        addPayload(null);
    }

    private CreateTableMsg(Msg msg) {
        super(msg);
        data = getPayload(0);
    }

    public static CreateTableMsg wrap(Msg msg) {
        return new CreateTableMsg(msg);
    }

    public int getTableId() {
        return data.getInt(TABLE_ID_OFFSET);
    }

    public void setTableId(int id) {
        data.putInt(TABLE_ID_OFFSET, id);
    }

    public int getStaleness() {
        return data.getInt(STALENESS_OFFSET);
    }

    public void setStaleness(int staleness) {
        data.putInt(STALENESS_OFFSET, staleness);
    }

    public int getRowType() {
        return data.getInt(ROW_TYPE_OFFSET);
    }

    public void setRowType(int rowType) {
        data.putInt(ROW_TYPE_OFFSET, rowType);
    }

    public int getRowCapacity() {
        return data.getInt(ROW_CAPACITY_OFFSET);
    }

    public void setRowCapacity(int rowCapacity) {
        data.putInt(ROW_CAPACITY_OFFSET, rowCapacity);
    }

    public boolean getOplogDenseSerialized() {
        return data.getInt(OPLOG_DENSE_SERIALIZED_OFFSET) != 0;
    }

    public void setOplogDenseSerialized(boolean oplogDenseSerialized) {
        data.putInt(OPLOG_DENSE_SERIALIZED_OFFSET, oplogDenseSerialized ? 1 : 0);
    }

    public int getRowOplogType() {
        return data.getInt(ROW_OPLOG_TYPE_OFFSET);
    }

    public void setRowOplogType(int rowCapacity) {
        data.putInt(ROW_OPLOG_TYPE_OFFSET, rowCapacity);
    }

    public int getDenseRowOplogCapacity() {
        return data.getInt(DENSE_ROW_OPLOG_CAPACITY_OFFSET);
    }

    public void setDenseRowOplogCapacity(int denseRowOplogCapacity) {
        data.putInt(DENSE_ROW_OPLOG_CAPACITY_OFFSET, denseRowOplogCapacity);
    }

    public Config getRowConfig() {
        ByteBuffer buffer = getPayload(1).duplicate();
        buffer.rewind();
        return Config.deserialize(buffer);
    }

    public void setRowConfig(Config rowConfig) {
        setPayload(1, rowConfig.serialize());
    }

    public Config getRowUpdateConfig() {
        getPayload(2).rewind();
        return Config.deserialize(getPayload(2));
    }

    public void setRowUpdateConfig(Config rowUpdateConfig) {
        setPayload(2, rowUpdateConfig.serialize());
    }

}