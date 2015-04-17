package org.petuum.ps.server;

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.petuum.ps.common.FactoryRegistry;
import org.petuum.ps.common.util.IntBox;
import org.petuum.ps.common.util.RecordBuff;
import org.petuum.ps.config.Config;
import org.petuum.ps.config.TableInfo;
import org.petuum.ps.row.Row;
import org.petuum.ps.row.RowFactory;
import org.petuum.ps.row.RowUpdate;
import org.petuum.ps.row.RowUpdateFactory;

import java.nio.ByteBuffer;

public class ServerTable {

    private static int kTmpRowBuffSizeInit = 512;
    private RowFactory rowFactory;
    private Config rowConfig;
    private RowUpdateFactory rowUpdateFactory;
    private Config rowUpdateConfig;
    private int tableId;
    private TableInfo table_info_;
    private TIntObjectMap<ServerRow> storage_;
    private TIntObjectIterator<ServerRow> storage_iter_;
    // used for appending rows to buffs
    private ByteBuffer tmp_row_buff_;
    private int tmp_row_buff_size_;
    private int curr_row_size_;

    public ServerTable(int tableId, TableInfo table_info, Config rowConfig, Config rowUpdateConfig) {
        this.table_info_ = table_info;
        this.tmp_row_buff_size_ = kTmpRowBuffSizeInit;
        this.storage_ = new TIntObjectHashMap<>();
        this.tableId = tableId;
        this.rowFactory = FactoryRegistry.getRowFactory(table_info.rowType);
        this.rowUpdateFactory = FactoryRegistry.getRowUpdateFactory(table_info.rowUpdateType);
        this.rowConfig = rowConfig;
        this.rowUpdateConfig = rowUpdateConfig;
    }

    public ServerRow findRow(int row_id) {
        return storage_.get(row_id);
    }

    public ServerRow createRow(int row_id) {
        Row row_data = this.rowFactory.createRow(this.rowConfig);
        storage_.put(row_id, new ServerRow(row_data));
        return storage_.get(row_id);
    }

    public boolean applyRowOpLog(int row_id, RowUpdate updates) {
        ServerRow server_row = storage_.get(row_id);
        if (server_row == null) {
            return false;
        }

        server_row.ApplyBatchInc(updates);
        return true;
    }

    public void initAppendTableToBuffs() {
        this.storage_iter_ = storage_.iterator();
        this.tmp_row_buff_ = ByteBuffer.allocate(kTmpRowBuffSizeInit);
    }

    public boolean appendTableToBuffs(int client_id_st,
                                      TIntObjectMap<RecordBuff> buffs, IntBox failed_client_id,
                                      boolean resume) {

        if (resume) {
            storage_iter_.advance();
            boolean append_row_suc = storage_iter_.value().AppendRowToBuffs(
                    client_id_st, buffs, tmp_row_buff_, curr_row_size_,
                    storage_iter_.key(), failed_client_id);
            if (!append_row_suc)
                return false;
            client_id_st = 0;
        }
        while (storage_iter_.hasNext()) {
            storage_iter_.advance();

            if (storage_iter_.value().NoClientSubscribed())
                continue;

            if (!storage_iter_.value().IsDirty())
                continue;

            storage_iter_.value().ResetDirty();
            tmp_row_buff_ = storage_iter_.value().Serialize();
            curr_row_size_ = tmp_row_buff_.capacity();

            if (curr_row_size_ > tmp_row_buff_size_) {
                tmp_row_buff_size_ = curr_row_size_;
            }

            boolean append_row_suc = storage_iter_.value().AppendRowToBuffs(
                    client_id_st, buffs, tmp_row_buff_, curr_row_size_,
                    storage_iter_.key(), failed_client_id);

            if (!append_row_suc) {
                return false;
            }
        }
        tmp_row_buff_.clear();
        return true;
    }

    public void GetPartialTableToSend(TIntObjectMap<ServerRow> rows_to_send,
                                      TIntIntMap client_size_map, int num_rows_threshold) {

    }

    public void AppendRowsToBuffsPartial(TIntObjectMap<RecordBuff> buffs,
                                         TIntObjectMap<ServerRow> rows_to_send) {

    }

    public void MakeSnapShotFileName(String snapshot_dir, int server_id,
                                     int table_id, int clock, StringBuffer filename) {
        filename.append(snapshot_dir).append("/server_table")
                .append(".server-").append(server_id).append(".table-")
                .append(table_id).append(".clock-").append(clock).append(".db");

    }

    void TakeSnapShot(String snapshot_dir, int server_id, int table_id, int clock) {
    }

    public void readSnapShot(String resume_dir, int server_id, int table_id, int clock) {
    }

    public RowFactory getRowFactory() {
        return this.rowFactory;
    }

    public RowUpdateFactory getRowUpdateFactory() {
        return this.rowUpdateFactory;
    }

    public Config getRowConfig() {
        return this.rowConfig;
    }

    public Config getRowUpdateConfig() {
        return this.rowUpdateConfig;
    }

}