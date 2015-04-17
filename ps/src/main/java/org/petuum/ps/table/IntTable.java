package org.petuum.ps.table;

import org.petuum.ps.client.AbstractClientTable;
import org.petuum.ps.row.int_.IntRowUpdate;
import org.petuum.ps.row.int_.SparseIntRowUpdate;

/**
 * Created by aqiao on 3/5/15.
 */
public class IntTable extends AbstractTable {

    private AbstractClientTable clientTable;

    public IntTable(AbstractClientTable clientTable) {
        super(clientTable);
        this.clientTable = clientTable;
    }

    public void threadInc(int rowId, int columnId, int update) {
        SparseIntRowUpdate rowUpdate = new SparseIntRowUpdate();
        rowUpdate.setUpdate(columnId, update);
        clientTable.threadBatchInc(rowId, rowUpdate);
    }

    public void threadBatchInc(int rowId, IntRowUpdate rowUpdate) {
        clientTable.threadBatchInc(rowId, rowUpdate.getCopy());
    }

    public void inc(int rowId, int columnId, int update) {
        SparseIntRowUpdate rowUpdate = new SparseIntRowUpdate();
        rowUpdate.setUpdate(columnId, update);
        clientTable.batchInc(rowId, rowUpdate);
    }

    public void batchInc(int rowId, IntRowUpdate rowUpdate) {
        clientTable.batchInc(rowId, rowUpdate.getCopy());
    }

}
