package org.petuum.ps.common.msg;

import org.petuum.ps.common.network.Msg;
import org.petuum.ps.config.Config;

import java.nio.ByteBuffer;

/**
 * Created by aqiao on 3/27/15.
 */
public class BgCreateTableMsg extends Msg {

    private static final int TABLE_ID_OFFSET = 0;
    private static final int STALENESS_OFFSET = TABLE_ID_OFFSET + INT_LENGTH;
    private static final int ROW_TYPE_OFFSET = STALENESS_OFFSET + INT_LENGTH;
    private static final int ROW_CAPACITY_OFFSET = ROW_TYPE_OFFSET + INT_LENGTH;
    private static final int PROCESS_CACHE_CAPACITY_OFFSET = ROW_CAPACITY_OFFSET + INT_LENGTH;
    private static final int THREAD_CACHE_CAPACITY_OFFSET = PROCESS_CACHE_CAPACITY_OFFSET + INT_LENGTH;
    private static final int OPLOG_CAPACITY_OFFSET = THREAD_CACHE_CAPACITY_OFFSET + INT_LENGTH;
    private static final int OPLOG_DENSE_SERIALIZED_OFFSET = OPLOG_CAPACITY_OFFSET + INT_LENGTH;
    private static final int ROW_OPLOG_TYPE_OFFSET = OPLOG_DENSE_SERIALIZED_OFFSET + INT_LENGTH;
    private static final int DENSE_ROW_OPLOG_CAPACITY_OFFSET = ROW_OPLOG_TYPE_OFFSET + INT_LENGTH;
    private static final int OPLOG_TYPE_OFFSET = DENSE_ROW_OPLOG_CAPACITY_OFFSET + INT_LENGTH;
    private static final int APPEND_ONLY_OPLOG_TYPE_OFFSET = OPLOG_TYPE_OFFSET + INT_LENGTH;
    private static final int APPEND_ONLY_BUFF_CAPACITY_OFFSET = APPEND_ONLY_OPLOG_TYPE_OFFSET + INT_LENGTH;
    private static final int PER_THREAD_APPEND_ONLY_BUFF_POOL_SIZE_OFFSET = APPEND_ONLY_BUFF_CAPACITY_OFFSET + INT_LENGTH;
    private static final int BG_APPLY_APPEND_OPLOG_FREQ_OFFSET = PER_THREAD_APPEND_ONLY_BUFF_POOL_SIZE_OFFSET + INT_LENGTH;
    private static final int PROCESS_STORAGE_TYPE_OFFSET = BG_APPLY_APPEND_OPLOG_FREQ_OFFSET + INT_LENGTH;
    private static final int NO_OPLOG_REPLAY_OFFSET = PROCESS_STORAGE_TYPE_OFFSET + INT_LENGTH;

    private static final int DATA_SIZE = NO_OPLOG_REPLAY_OFFSET + INT_LENGTH;

    private ByteBuffer data;

    public BgCreateTableMsg() {
        super(MsgType.BG_CREATE_TABLE);
        data = ByteBuffer.allocate(DATA_SIZE);
        addPayload(data);
        addPayload(null);
        addPayload(null);
    }

    private BgCreateTableMsg(Msg msg) {
        super(msg);
        data = getPayload(0);
    }

    public static BgCreateTableMsg wrap(Msg msg) {
        return new BgCreateTableMsg(msg);
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

    public int getProcessCacheCapacity() {
        return data.getInt(PROCESS_CACHE_CAPACITY_OFFSET);
    }

    public void setProcessCacheCapacity(int processCacheCapacity) {
        data.putInt(PROCESS_CACHE_CAPACITY_OFFSET, processCacheCapacity);
    }

    public int getThreadCacheCapacity() {
        return data.getInt(THREAD_CACHE_CAPACITY_OFFSET);
    }

    public void setThreadCacheCapacity(int threadCacheCapacity) {
        data.putInt(THREAD_CACHE_CAPACITY_OFFSET, threadCacheCapacity);
    }

    public int getOplogCapacity() {
        return data.getInt(OPLOG_CAPACITY_OFFSET);
    }

    public void setOplogCapacity(int oplogCapacity) {
        data.putInt(OPLOG_CAPACITY_OFFSET, oplogCapacity);
    }

    public boolean getOplogDenseSerialized() {
        return data.getInt(OPLOG_DENSE_SERIALIZED_OFFSET) != 0;
    }

    public void setOplogDenseSerialized(boolean oplogDenseSerialized) {
        data.putInt(OPLOG_DENSE_SERIALIZED_OFFSET, oplogDenseSerialized ? 1
                : 0);
    }

    public int getRowOplogType() {
        return data.getInt(ROW_OPLOG_TYPE_OFFSET);
    }

    public void setRowOplogType(int rowOplogType) {
        data.putInt(ROW_OPLOG_TYPE_OFFSET, rowOplogType);
    }

    public int getDenseRowOplogCapacity() {
        return data.getInt(DENSE_ROW_OPLOG_CAPACITY_OFFSET);
    }

    public void setDenseRowOplogCapacity(int denseRowOplogCapacity) {
        data.putInt(DENSE_ROW_OPLOG_CAPACITY_OFFSET, denseRowOplogCapacity);
    }

    public int getOplogType() {
        return data.getInt(OPLOG_TYPE_OFFSET);
    }

    public void setOplogType(int oplogType) {
        data.putInt(OPLOG_TYPE_OFFSET, oplogType);
    }

    public int getAppendOnlyOplogType() {
        return data.getInt(APPEND_ONLY_OPLOG_TYPE_OFFSET);
    }

    public void setAppendOnlyOplogType(int appendOnlyOplogType) {
        data.putInt(APPEND_ONLY_OPLOG_TYPE_OFFSET, appendOnlyOplogType);
    }

    public int getAppendOnlyBuffCapacity() {
        return data.getInt(APPEND_ONLY_BUFF_CAPACITY_OFFSET);
    }

    public void setAppendOnlyBuffCapacity(int appendOnlyBuffCapacity) {
        data.putInt(APPEND_ONLY_BUFF_CAPACITY_OFFSET,
                appendOnlyBuffCapacity);
    }

    public int getPerThreadAppendOnlyBuffPoolSize() {
        return data.getInt(PER_THREAD_APPEND_ONLY_BUFF_POOL_SIZE_OFFSET);
    }

    public void setPerThreadAppendOnlyBuffPoolSize(
            int perThreadAppendOnlyBuffPoolSize) {
        data.putInt(PER_THREAD_APPEND_ONLY_BUFF_POOL_SIZE_OFFSET,
                perThreadAppendOnlyBuffPoolSize);
    }

    public int getBgApplyAppendOplogFreq() {
        return data.getInt(BG_APPLY_APPEND_OPLOG_FREQ_OFFSET);
    }

    public void setBgApplyAppendOplogFreq(int bgApplyAppendOplogFreq) {
        data.putInt(BG_APPLY_APPEND_OPLOG_FREQ_OFFSET,
                bgApplyAppendOplogFreq);
    }

    public int getProcessStorageType() {
        return data.getInt(PROCESS_STORAGE_TYPE_OFFSET);
    }

    public void setProcessStorageType(int processStorageType) {
        data.putInt(PROCESS_STORAGE_TYPE_OFFSET, processStorageType);
    }

    public boolean getNoOplogReplay() {
        return data.getInt(NO_OPLOG_REPLAY_OFFSET) != 0;
    }

    public void setNoOplogReplay(boolean noOplogReplay) {
        data.putInt(NO_OPLOG_REPLAY_OFFSET, noOplogReplay ? 1 : 0);
    }

    public Config getRowConfig() {
        getPayload(1).rewind();
        return Config.deserialize(getPayload(1));
    }

    public void setRowConfig(Config rowConfig) {
        setPayload(1, rowConfig.serialize());
    }

    public Config getRowUpdateConfig() {
        getPayload(2).rewind();
        return Config.deserialize(getPayload(2));
    }

    public void setRowUpdateConfig(Config rowConfig) {
        setPayload(2, rowConfig.serialize());
    }
}