package org.petuum.ps.table;

import org.petuum.ps.client.AbstractClientTable;
import org.petuum.ps.row.float_.FloatRowUpdate;
import org.petuum.ps.row.float_.SparseFloatRowUpdate;

/**
 * Created by aqiao on 3/5/15.
 */
public class FloatTable extends AbstractTable {

    private AbstractClientTable clientTable;

    public FloatTable(AbstractClientTable clientTable) {
        super(clientTable);
        this.clientTable = clientTable;
    }

    public void threadInc(int rowId, int columnId, float update) {
        SparseFloatRowUpdate rowUpdate = new SparseFloatRowUpdate();
        rowUpdate.setUpdate(columnId, update);
        clientTable.threadBatchInc(rowId, rowUpdate);
    }

    public void threadBatchInc(int rowId, FloatRowUpdate rowUpdate) {
        clientTable.threadBatchInc(rowId, rowUpdate.getCopy());
    }

    public void inc(int rowId, int columnId, float update) {
        SparseFloatRowUpdate rowUpdate = new SparseFloatRowUpdate();
        rowUpdate.setUpdate(columnId, update);
        clientTable.batchInc(rowId, rowUpdate);
    }

    public void batchInc(int rowId, FloatRowUpdate rowUpdate) {
        clientTable.batchInc(rowId, rowUpdate.getCopy());
    }

}
