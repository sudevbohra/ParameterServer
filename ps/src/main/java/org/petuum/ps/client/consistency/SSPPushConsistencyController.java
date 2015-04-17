package org.petuum.ps.client.consistency;

/**
 * @author yihuaf
 * 
 */

import org.petuum.ps.client.ClientRow;
import org.petuum.ps.client.ThreadTable;
import org.petuum.ps.client.oplog.Oplog;
import org.petuum.ps.client.oplog.TableOpLogIndex;
import org.petuum.ps.client.storage.ProcessStorage;
import org.petuum.ps.client.thread.BgWorkers;
import org.petuum.ps.client.thread.ThreadContext;
import org.petuum.ps.config.TableConfig;
import org.petuum.ps.row.Row;

public class SSPPushConsistencyController extends SSPConsistencyController {

    private static final int kMaxPendingAsyncGetCnt = 256;

    private ThreadLocal<Integer> pending_async_get_cnt_;

    public SSPPushConsistencyController(TableConfig tableConfig, int tableId,
            ProcessStorage processStorage, Oplog oplog,
            ThreadLocal<ThreadTable> threadCache, TableOpLogIndex oplogIndex) {
        super(tableConfig, tableId, processStorage, oplog, threadCache,
                oplogIndex);
        this.pending_async_get_cnt_ = new ThreadLocal<Integer>();
    }

    private void getAsyncHelper(int rowId, boolean forced) {
        // Look for row_id in process_storage_.
        int stalestClock = ThreadContext.getClock();
        int pendingAsyncGetCount = pending_async_get_cnt_.get();

        if (pendingAsyncGetCount == kMaxPendingAsyncGetCnt) {
            BgWorkers.getAsyncRowRequestReply();
            pending_async_get_cnt_.set(pendingAsyncGetCount--);
        }

        BgWorkers.requestRowAsync(tableId, rowId, stalestClock, true);
        pending_async_get_cnt_.set(pendingAsyncGetCount++);
    }

    @Override
    public void getAsyncForced(int rowId) {
        getAsyncHelper(rowId, true);
    }

    @Override
    public void getAsync(int rowId) {

        if (processStorage.getRow(rowId) != null) {
            return;
        }

        getAsyncHelper(rowId, false);
    }

    @Override
    public void waitPendingAsnycGet() {

        if (pending_async_get_cnt_.get() == 0) {
            return;
        }

        while (pending_async_get_cnt_.get() > 0) {
            BgWorkers.getAsyncRowRequestReply();
            pending_async_get_cnt_.set(pending_async_get_cnt_.get());
        }

    }

    // Check freshness; make request and block if too stale or row_id not found
    // in storage.
    @Override
    public ClientRow get(int rowId) {
        // System.out.println("SSPConsistencyController get id=" +
        // ThreadContext.getId() + " tableId=" + this.tableId + " rowId=" +
        // rowId);
        int stalestClock = Math.max(0, ThreadContext.getClock()
                - this.tableConfig.getStaleness());

        if (ThreadContext.getCachedSystemClock() < stalestClock) {
            int systemClock = BgWorkers.getSystemClock();
            if (systemClock < stalestClock) {
                BgWorkers.waitSystemClock(stalestClock);
                systemClock = BgWorkers.getSystemClock();
            }
            ThreadContext.setCachedSystemClock(systemClock);
        }

        ClientRow clientRow = this.processStorage.getRow(rowId);

        if (clientRow != null) {
            return clientRow;
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
    public Row threadGet(int rowId) {

        int stalestClock = Math.max(0, ThreadContext.getClock()
                - this.tableConfig.getStaleness());

        if (ThreadContext.getCachedSystemClock() < stalestClock) {
            int systemClock = BgWorkers.getSystemClock();
            if (systemClock < stalestClock) {
                BgWorkers.waitSystemClock(stalestClock);
                systemClock = BgWorkers.getSystemClock();
            }
            ThreadContext.setCachedSystemClock(systemClock);
        }

        Row rowData = this.threadCache.get().getRow(rowId);
        if (rowData != null) {
            return rowData;
        }

        ClientRow clientRow = this.processStorage.getRow(rowId);

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

}
