package org.petuum.ps.client;

import org.petuum.ps.row.Row;
import org.petuum.ps.row.RowUpdate;

public abstract class AbstractClientTable {

    public AbstractClientTable() {
    }

    public void close() {
    }

    public abstract void registerThread();

    public abstract void getAsyncForced(int row_id);

    public abstract void getAsync(int row_id);

    public abstract void waitPendingAsyncGet();

    public abstract Row threadGet(int row_id);

    public abstract void threadBatchInc(int rowId, RowUpdate rowUpdate);

    public abstract void flushThreadCache();

    public abstract ClientRow get(int row_id);

    public abstract void batchInc(int rowId, RowUpdate rowUpdate);

    public abstract void clock();

    public abstract int getRowType();
}
