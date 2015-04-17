package org.petuum.ps.client.oplog;

import org.petuum.ps.row.RowUpdate;

/**
 * Created by aqiao on 3/17/15.
 */
public interface OplogPartition {
    public RowUpdate createRowUpdate(int rowId);

    public RowUpdate getRowUpdate(int rowId);

    public RowUpdate removeRowUpdate(int rowId);
}
