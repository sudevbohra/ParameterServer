package org.petuum.ps.table;

import org.petuum.ps.client.AbstractClientTable;
import org.petuum.ps.row.double_.DoubleRowUpdate;
import org.petuum.ps.row.double_.SparseDoubleRowUpdate;

/**
 * Created by aqiao on 2/20/15.
 */
public class DoubleTable extends AbstractTable {

    private AbstractClientTable clientTable;

    public DoubleTable(AbstractClientTable clientTable) {
        super(clientTable);
        this.clientTable = clientTable;
    }

    public void threadInc(int rowId, int columnId, double update) {
        SparseDoubleRowUpdate rowUpdate = new SparseDoubleRowUpdate();
        rowUpdate.setUpdate(columnId, update);
        clientTable.threadBatchInc(rowId, rowUpdate);
    }

    public void threadBatchInc(int rowId, DoubleRowUpdate rowUpdate) {
        clientTable.threadBatchInc(rowId, rowUpdate.getCopy());
    }

    public void inc(int rowId, int columnId, double update) {
        SparseDoubleRowUpdate rowUpdate = new SparseDoubleRowUpdate();
        rowUpdate.setUpdate(columnId, update);
        clientTable.batchInc(rowId, rowUpdate);
    }

    public void batchInc(int rowId, DoubleRowUpdate rowUpdate) {
        clientTable.batchInc(rowId, rowUpdate.getCopy());
    }

}
