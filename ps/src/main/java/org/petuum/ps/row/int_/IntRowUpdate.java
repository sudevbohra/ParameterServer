package org.petuum.ps.row.int_;

import org.petuum.ps.row.RowUpdate;

/**
 * Created by aqiao on 2/20/15.
 */
public interface IntRowUpdate extends RowUpdate {
    public int getUpdate(int columnId);

    public void setUpdate(int columnId, int update);

    public void incUpdate(int columnId, int update);

    public IntUpdateIterator iterator();
}
