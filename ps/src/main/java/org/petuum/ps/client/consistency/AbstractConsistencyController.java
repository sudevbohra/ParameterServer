package org.petuum.ps.client.consistency;

import org.petuum.ps.client.ClientRow;
import org.petuum.ps.client.storage.ProcessStorage;
import org.petuum.ps.row.Row;
import org.petuum.ps.row.RowUpdate;

public abstract class AbstractConsistencyController {

    protected ProcessStorage processStorage;
    protected int tableId;

    public AbstractConsistencyController(int tableId, ProcessStorage processStorage) {
        this.processStorage = processStorage;
        this.tableId = tableId;
    }

    public void close() {
    }

    public abstract void getAsyncForced(int rowId);

    public abstract void getAsync(int rowId);

    public abstract void waitPendingAsnycGet();

    public abstract ClientRow get(int rowId);

    public abstract void batchInc(int rowId, RowUpdate rowUpdate);

    public abstract Row threadGet(int rowId);

    public abstract void threadBatchInc(int rowId, RowUpdate rowUpdate);

    public abstract void flushThreadCache();

    public abstract void clock();

}
