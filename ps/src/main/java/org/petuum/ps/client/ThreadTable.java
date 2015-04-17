package org.petuum.ps.client;

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.petuum.ps.client.oplog.Oplog;
import org.petuum.ps.client.oplog.TableOpLogIndex;
import org.petuum.ps.client.storage.ProcessStorage;
import org.petuum.ps.client.thread.GlobalContext;
import org.petuum.ps.common.FactoryRegistry;
import org.petuum.ps.config.TableConfig;
import org.petuum.ps.row.Row;
import org.petuum.ps.row.RowUpdate;
import org.petuum.ps.row.RowUpdateFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class ThreadTable {

    private ArrayList<TIntSet> oplogIndex;
    private TIntObjectMap<Row> rowStorage;
    private TIntObjectMap<RowUpdate> oplogMap;

    private UpdateOpLogClockFunc updateOpLogClock;
    private RowUpdateFactory rowUpdateFactory;

    private TableConfig tableConfig;

    public ThreadTable(TableConfig tableConfig) {
        this.tableConfig = tableConfig;
        this.rowUpdateFactory = FactoryRegistry.getRowUpdateFactory(tableConfig.getRowUpdateType());
        this.oplogIndex = new ArrayList<>(
                GlobalContext.getNumCommChannelsPerClient());
        for (int i = 0; i < GlobalContext.getNumCommChannelsPerClient(); i++) {
            oplogIndex.add(new TIntHashSet());
        }
        this.rowStorage = new TIntObjectHashMap<>();
        this.oplogMap = new TIntObjectHashMap<>();

        this.updateOpLogClock = new UpdateOpLogClockNoOp();
    }

    public void indexUpdate(int rowId) {
        int partitionNum = GlobalContext.getPartitionCommChannelIndex(rowId);
        this.oplogIndex.get(partitionNum).add(rowId);
    }

    public Row getRow(int rowId) {
        if (this.rowStorage.containsKey(rowId)) {
            return this.rowStorage.get(rowId);
        } else {
            return null;
        }
    }

    public void insertRow(int rowId, Row row) {
        rowStorage.put(rowId, row);
        if (oplogMap.containsKey(rowId)) {
            RowUpdate rowOplog = oplogMap.get(rowId);
            row.applyRowUpdate(rowOplog);
        }
    }

    public void flushCache(ProcessStorage processStorage, Oplog tableOplog) {
        flushCacheOpLog(processStorage, tableOplog);
        rowStorage.clear();
    }

    private void flushCacheOpLog(ProcessStorage processStorage, Oplog tableOplog) {

        for (TIntObjectIterator<RowUpdate> oplogIter = oplogMap
                .iterator(); oplogIter.hasNext(); ) {
            oplogIter.advance();
            int rowId = oplogIter.key();

            tableOplog.lockRow(rowId);
            try {
                RowUpdate rowOplog = tableOplog.getRowUpdate(rowId);
                if (rowOplog == null) {
                    rowOplog = tableOplog.createRowUpdate(rowId);
                }
                updateOpLogClock.invoke(rowOplog);

                rowOplog.addRowUpdate(oplogIter.value());

                ClientRow clientRow = processStorage.getRow(rowId);
                if (clientRow != null) {
                    clientRow.getRowDataPtr().applyRowUpdate(oplogIter.value());
                }
            } finally {
                tableOplog.unlockRow(rowId);
            }
        }
        oplogMap.clear();
    }

    public void batchInc(int rowId, RowUpdate rowUpdate) {

        RowUpdate rowOplog;
        if (!oplogMap.containsKey(rowId)) {
            rowOplog = rowUpdateFactory.create(this.tableConfig.getRowUpdateConfig());
            oplogMap.put(rowId, rowOplog);
        } else {
            rowOplog = oplogMap.get(rowId);
        }

        rowOplog.addRowUpdate(rowUpdate);

        if (rowStorage.containsKey(rowId)) {
            rowStorage.get(rowId).applyRowUpdateUnlocked(rowUpdate);
        }
    }

    public void flushOpLogIndex(TableOpLogIndex tableOplogIndex) {
        for (int i = 0; i < GlobalContext.getNumCommChannelsPerClient(); ++i) {
            TIntSet partitionOplogIndex = this.oplogIndex.get(i);
            tableOplogIndex.addIndex(i, partitionOplogIndex);
            this.oplogIndex.get(i).clear();
        }
    }

    private static abstract class UpdateOpLogClockFunc {
        public abstract void invoke(RowUpdate rowOplog);
    }

    private class UpdateOpLogClockNoOp extends UpdateOpLogClockFunc {
        public void invoke(RowUpdate rowOplog) {
        }
    }
}
