package org.petuum.ps.client.consistency;

import org.petuum.ps.client.ClientRow;
import org.petuum.ps.client.ThreadTable;
import org.petuum.ps.client.oplog.Oplog;
import org.petuum.ps.client.oplog.TableOpLogIndex;
import org.petuum.ps.client.storage.ProcessStorage;
import org.petuum.ps.client.thread.BgWorkers;
import org.petuum.ps.client.thread.ThreadContext;
import org.petuum.ps.config.TableConfig;
import org.petuum.ps.row.Row;
import org.petuum.ps.row.RowUpdate;

public class SSPConsistencyController extends AbstractConsistencyController {

    protected TableConfig tableConfig;
    protected ThreadLocal<ThreadTable> threadCache;
    private TableOpLogIndex oplogIndex;
    private Oplog oplog;

    public SSPConsistencyController(TableConfig tableConfig, int tableId,
                                    ProcessStorage processStorage, Oplog oplog,
                                    ThreadLocal<ThreadTable> threadCache,
                                    TableOpLogIndex oplogIndex) {
        super(tableId, processStorage);
        this.tableConfig = tableConfig;
        this.threadCache = threadCache;
        this.oplogIndex = oplogIndex;
        this.oplog = oplog;
    }

    @Override
    public void getAsyncForced(int rowId) {
    }

    @Override
    public void getAsync(int rowId) {
    }

    @Override
    public void waitPendingAsnycGet() {
    }

    @Override
    public ClientRow get(int rowId) {
        // System.out.println("SSPConsistencyController get id=" +
        // ThreadContext.getId() + " tableId=" + this.tableId + " rowId=" +
        // rowId);
        int stalestClock = Math.max(0, ThreadContext.getClock()
                - this.tableConfig.getStaleness());

        ClientRow clientRow = this.processStorage.getRow(rowId);

        if (clientRow != null) {
            // Found it! Check staleness.
            int clock = clientRow.getClock();
            if (clock >= stalestClock) {
                return clientRow;
            }
        }

        // Didn't find rowId that's fresh enough in this.processStorage.
        // Fetch from server.
        int numFetches = 0;
        do {
            // System.out.println("SSPConsistencyController request id=" +
            // ThreadContext.getId() + " tableId=" + tableId + " rowId=" + rowId
            // + " stalestClock=" + stalestClock);
            BgWorkers.requestRow(this.tableId, rowId, stalestClock);
            // System.out.println("SSPConsistencyController receive id=" +
            // ThreadContext.getId() + " tableId=" + tableId +" rowId=" + rowId
            // + " stalestClock=" + stalestClock);

            // fetch again
            clientRow = this.processStorage.getRow(rowId);
            // TODO (jinliang):
            // It's possible that the application thread does not find the row
            // that
            // the bg thread has just inserted. In practice, this shouldn't be
            // an issue.
            // We'll fix it if it turns out there are too many misses.
            ++numFetches;
            assert (numFetches <= 3); // to prevent infinite loop
        } while (clientRow == null);

        assert (clientRow.getClock() >= stalestClock);

        return clientRow;
    }

    @Override
    public void batchInc(int rowId, RowUpdate rowUpdate) {

        this.threadCache.get().indexUpdate(rowId);

        this.oplog.lockRow(rowId);
        try {
            RowUpdate rowOplog = this.oplog.getRowUpdate(rowId);
            if (rowOplog == null) {
                rowOplog = this.oplog.createRowUpdate(rowId);
            }

            rowOplog.addRowUpdate(rowUpdate);

            ClientRow clientRow = this.processStorage.getRow(rowId);
            if (clientRow != null) {
                clientRow.getRowDataPtr().applyRowUpdate(rowUpdate);
            }
        } finally {
            this.oplog.unlockRow(rowId);
        }
    }

    @Override
    public Row threadGet(int rowId) {
        Row rowData = this.threadCache.get().getRow(rowId);
        if (rowData != null) {
            return rowData;
        }

        ClientRow clientRow = this.processStorage.getRow(rowId);

        int stalestClock = Math.max(0, ThreadContext.getClock()
                - this.tableConfig.getStaleness());
        if (clientRow != null) {
            // Found it! Check staleness.
            int clock = clientRow.getClock();
            if (clock >= stalestClock) {
                Row tmpRowData = clientRow.getRowDataPtr();
                this.threadCache.get().insertRow(rowId, tmpRowData);
                rowData = this.threadCache.get().getRow(rowId);
                assert (rowData != null);
                return rowData;
            }
        }

        // Didn't find rowId that's fresh enough in this.processStorage.
        // Fetch from server.
        int numFetches = 0;
        do {
            BgWorkers.requestRow(this.tableId, rowId, stalestClock);

            // fetch again
            clientRow = this.processStorage.getRow(rowId);
            // TODO (jinliang):
            // It's possible that the application thread does not find the
            // row that
            // the bg thread has just inserted. In practice, this shouldn't
            // be an issue.
            // We'll fix it if it turns out there are too many misses.
            ++numFetches;
            assert (numFetches <= 3); // to prevent infinite loop
        } while (clientRow == null);

        assert (clientRow.getClock() >= stalestClock);

        Row tmpRowData = clientRow.getRowDataPtr();
        this.threadCache.get().insertRow(rowId, tmpRowData);
        rowData = this.threadCache.get().getRow(rowId);
        assert (rowData != null);
        return rowData;
    }

    @Override
    public void threadBatchInc(int rowId, RowUpdate rowUpdate) {
        this.threadCache.get().batchInc(rowId, rowUpdate);
    }

    @Override
    public void flushThreadCache() {
        this.threadCache.get().flushCache(this.processStorage, this.oplog);
    }

    @Override
    public void clock() {
        // System.out.println("SSPConsistencyController clock id=" +
        // ThreadContext.getId() + " tableId=" + this.tableId + " clock=" +
        // ThreadContext.getClock());
        this.threadCache.get().flushCache(this.processStorage, this.oplog);
        this.threadCache.get().flushOpLogIndex(this.oplogIndex);
    }

}
