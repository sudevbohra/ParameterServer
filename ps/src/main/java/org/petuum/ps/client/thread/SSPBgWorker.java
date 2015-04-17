package org.petuum.ps.client.thread;

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntObjectMap;

import org.petuum.ps.client.ClientRow;
import org.petuum.ps.client.ClientTable;
import org.petuum.ps.client.SSPClientRow;
import org.petuum.ps.client.oplog.Oplog;
import org.petuum.ps.common.util.IntBox;
import org.petuum.ps.common.util.PtrBox;
import org.petuum.ps.row.Row;
import org.petuum.ps.row.RowUpdate;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CyclicBarrier;

public class SSPBgWorker extends AbstractBgWorker {

    public SSPBgWorker(int id, int commChannelIdx,
                       TIntObjectMap<ClientTable> tables, CyclicBarrier initBarrier,
                       CyclicBarrier createTableBarrier) {
        super(id, commChannelIdx, tables, initBarrier, createTableBarrier);
    }

    @Override
    protected void createRowRequestOpLogMgr() {
        this.rowRequestOplogMgr = new SSPRowRequestOpLogMgr();
    }

    protected boolean getRowOpLog(Oplog tableOplog, int rowId,
                                  PtrBox<RowUpdate> rowOplogPtr) {
        rowOplogPtr.value = tableOplog.removeRowUpdate(rowId);
        return rowOplogPtr != null;
    }

    @Override
    protected void prepareBeforeInfiniteLoop() {
    }

    @Override
    protected void finalizeTableStats() {
    }

    @Override
    protected long resetBgIdleMilli() {
        return 0;
    }

    @Override
    protected long bgIdleWork() {
        return 0;
    }

    @Override
    protected ClientRow createClientRow(int clock, Row rowData) {
        return new SSPClientRow(clock, rowData, true);
    }

    @Override
    protected BgOpLog prepareOpLogsToSend() {
        BgOpLog bgOplog = new BgOpLog();
        for (TIntObjectIterator<ClientTable> tablePair = this.tables.iterator(); tablePair
                .hasNext(); ) {
            tablePair.advance();
            int tableId = tablePair.key();

            if (tablePair.value().isNoOplogReplay()) {
                prepareOpLogsNormalNoReplay(tableId, tablePair.value());
            } else {
                BgOpLogPartition bgTableOplog = prepareOpLogsNormal(tableId, tablePair.value());
                bgOplog.Add(tableId, bgTableOplog);
            }

            finalizeOpLogMsgStats(tableId, tableNumBytesByServer,
                    serverTableOplogSizeMap);
        }
        return bgOplog;
    }

    protected BgOpLogPartition prepareOpLogsNormal(int tableId,
                                                   ClientTable table) {
        Oplog tableOplog = table.getOplog();

        // get OpLog index
        Map<Integer, Boolean> newTableOplogIndexPtr = table
                .getAndResetOpLogIndex(myCommChannelIdx);

        BgOpLogPartition bgTableOplog = new BgOpLogPartition(tableId, myCommChannelIdx);

        for (Integer serverId : this.serverIds) {
            // Reset size to 0
            tableNumBytesByServer.put(serverId, 0);
        }

        // TODO: Change to forEach???
        int count = 0;
//        TIntObjectIterator<Boolean> oplogIndexIter = newTableOplogIndexPtr
//                .iterator();
        Iterator<Entry<Integer, Boolean>> oplogIndexIter = newTableOplogIndexPtr.entrySet().iterator();
        while (oplogIndexIter.hasNext()) {
            //oplogIndexIter.advance();
            Entry<Integer, Boolean> entry = oplogIndexIter.next();
            int rowId = entry.getKey();
            PtrBox<RowUpdate> rowOplog = new PtrBox<>();
            boolean found = getRowOpLog(tableOplog, rowId, rowOplog);
            if (!found)
                continue;

            if (found && (rowOplog.value == null))
                continue;

            countRowOpLogToSend(rowId, rowOplog.value, tableNumBytesByServer,
                    bgTableOplog);
            count++;
        }
        return bgTableOplog;
    }

    void prepareOpLogsNormalNoReplay(int tableId, ClientTable table) {

        Oplog tableOplog = table.getOplog();

        if (!this.rowOplogSerializerMap.containsKey(tableId)) {
            RowOpLogSerializer rowOplogSerializer = new RowOpLogSerializer(myCommChannelIdx);
            this.rowOplogSerializerMap.put(tableId, rowOplogSerializer);
        }
        RowOpLogSerializer rowOplogSerializer = this.rowOplogSerializerMap
                .get(tableId);

        // get OpLog index
        Map<Integer, Boolean> newTableOplogIndexPtr = table
                .getAndResetOpLogIndex(myCommChannelIdx);

//        TIntObjectIterator<Boolean> oplogIndexIter = newTableOplogIndexPtr
//                .iterator();
        Iterator<Entry<Integer, Boolean>> oplogIndexIter = newTableOplogIndexPtr.entrySet().iterator();
        
        while (oplogIndexIter.hasNext()) {
            //oplogIndexIter.advance();
            int rowId = oplogIndexIter.next().getKey();
            tableOplog.lockRow(rowId);
            try {
                RowUpdate rowOplog = tableOplog.getRowUpdate(rowId);

                if (rowOplog == null)
                    continue;

                rowOplogSerializer.appendRowOpLog(rowId, rowOplog);
                rowOplog.clear();
            } finally {
                tableOplog.unlockRow(rowId);
            }
        }

        for (Integer serverId : this.serverIds) {
            // Reset size to 0
            tableNumBytesByServer.put(serverId, 0);
        }
        rowOplogSerializer.getServerTableSizeMap(tableNumBytesByServer);
    }

    @Override
    protected void trackBgOpLog(BgOpLog bgOplog) {
        this.rowRequestOplogMgr.addOpLog(this.version, bgOplog);
        ++this.version;
        this.rowRequestOplogMgr.informVersionInc();
    }

    @Override
    protected void checkAndApplyOldOpLogsToRowData(int tableId, int rowId,
                                                   int version, Row rowData) {
        if (version + 1 < this.version) {
            int versionSt = version + 1;
            int versionEnd = this.version - 1;
            applyOldOpLogsToRowData(tableId, rowId, versionSt, versionEnd, rowData);
        }
    }

    protected void applyOldOpLogsToRowData(int tableId, int rowId,
                                           int versionSt, int versionEnd, Row rowData) {
        BgOpLog bgOplog = this.rowRequestOplogMgr.opLogIterInit(versionSt,
                versionEnd);
        IntBox oplogVersion = new IntBox(versionSt);

        while (bgOplog != null) {
            BgOpLogPartition bgOplogPartition = bgOplog.get(tableId);
            // OpLogs that are after (exclusively) version should be applied
            RowUpdate rowOplog = bgOplogPartition.findOpLog(rowId);
            if (rowOplog != null) {
                rowData.applyRowUpdate(rowOplog);
            }
            bgOplog = this.rowRequestOplogMgr.opLogIterNext(oplogVersion);
        }
    }

}
