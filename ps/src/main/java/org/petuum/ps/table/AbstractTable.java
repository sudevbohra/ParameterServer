package org.petuum.ps.table;

import org.petuum.ps.client.AbstractClientTable;

/**
 * Created by aqiao on 2/20/15.
 */
public class AbstractTable {

    protected AbstractClientTable clientTable;

    public AbstractTable(AbstractClientTable clientTable) {
        this.clientTable = clientTable;
    }

    public void getAsyncForced(int rowId) {
        clientTable.getAsyncForced(rowId);
    }

    public void getAsync(int rowId) {
        clientTable.getAsync(rowId);
    }

    public void waitPendingAsyncGet() {
        clientTable.waitPendingAsyncGet();
    }

    public void threadGet(int rowId) {
        clientTable.threadGet(rowId);
    }

    public void flushThreadCache() {
        clientTable.flushThreadCache();
    }

    @SuppressWarnings("unchecked")
    public <RowType> RowType get(int rowId) {
        return (RowType) clientTable.get(rowId).getRowDataPtr();
    }

}
