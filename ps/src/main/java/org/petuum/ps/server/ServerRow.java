package org.petuum.ps.server;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import org.petuum.ps.common.util.IntBox;
import org.petuum.ps.common.util.RecordBuff;
import org.petuum.ps.row.Row;
import org.petuum.ps.row.RowUpdate;

import java.nio.ByteBuffer;

public class ServerRow {

    private CallBackSubs callback_subs_;
    private Row row_data_;
    private int num_clients_subscribed_;
    private boolean dirty_;

    public ServerRow() {
        this.row_data_ = null;
        this.num_clients_subscribed_ = 0;
        this.dirty_ = false;
    }

    public ServerRow(Row row_data) {
        this.row_data_ = row_data;
        this.num_clients_subscribed_ = 0;
        this.dirty_ = false;
    }

    public ServerRow(ServerRow other) {
        this.row_data_ = other.row_data_;
        this.num_clients_subscribed_ = other.num_clients_subscribed_;
        this.dirty_ = other.dirty_;

    }

    public void ApplyBatchInc(RowUpdate update_batch) {
        row_data_.applyRowUpdateUnlocked(update_batch);
        dirty_ = true;
    }

    public int SerializedSize() {
        return row_data_.getSerializedSize();
    }

    public ByteBuffer Serialize() {
        return row_data_.serialize();
    }

    public void Subscribe(int client_id) {
        if (callback_subs_.Subscribe(client_id))
            ++num_clients_subscribed_;
    }

    public boolean NoClientSubscribed() {
        return (num_clients_subscribed_ == 0);
    }

    public void Unsubscribe(int client_id) {
        if (callback_subs_.Unsubscribe(client_id))
            --num_clients_subscribed_;
    }

    public boolean AppendRowToBuffs(int client_id_st,
                                    TIntObjectMap<RecordBuff> buffs, ByteBuffer row_data, int row_size,
                                    int row_id, IntBox failed_client_id) {
        return callback_subs_.AppendRowToBuffs(client_id_st, buffs, row_data,
                row_size, row_id, failed_client_id);
    }

    public boolean IsDirty() {
        return dirty_;
    }

    public void ResetDirty() {
        dirty_ = false;
    }

    public void AccumSerializedSizePerClient(TIntIntMap client_size_map) {
        callback_subs_.AccumSerializedSizePerClient(client_size_map,
                SerializedSize());
    }

    public void AppendRowToBuffs(TIntObjectMap<RecordBuff> buffs,
                                 ByteBuffer row_data, int row_size, int row_id) {
        callback_subs_.AppendRowToBuffs(buffs, row_data, row_size, row_id);
    }

}
