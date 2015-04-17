package org.petuum.ps.table;

import org.petuum.ps.client.AbstractClientTable;
import org.petuum.ps.row.long_.LongRowUpdate;
import org.petuum.ps.row.long_.SparseLongRowUpdate;

/**
 * Created by aqiao on 3/5/15.
 */
public class LongTable extends AbstractTable {

    private AbstractClientTable clientTable;

    public LongTable(AbstractClientTable clientTable) {
        super(clientTable);
        this.clientTable = clientTable;
    }

    public void threadInc(int rowId, int columnId, int update) {
        SparseLongRowUpdate rowUpdate = new SparseLongRowUpdate();
        rowUpdate.setUpdate(columnId, update);
        clientTable.threadBatchInc(rowId, rowUpdate);
    }

    public void threadBatchInc(int rowId, LongRowUpdate rowUpdate) {
        clientTable.threadBatchInc(rowId, rowUpdate.getCopy());
    }

    public void inc(int rowId, int columnId, int update) {
        SparseLongRowUpdate rowUpdate = new SparseLongRowUpdate();
        rowUpdate.setUpdate(columnId, update);
        clientTable.batchInc(rowId, rowUpdate);
    }

    public void batchInc(int rowId, LongRowUpdate rowUpdate) {
        clientTable.batchInc(rowId, rowUpdate.getCopy());
    }

}
