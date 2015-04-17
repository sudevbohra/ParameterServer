package org.petuum.ps.row.long_;

import org.petuum.ps.row.RowUpdate;

/**
 * Created by aqiao on 2/20/15.
 */
public interface LongRowUpdate extends RowUpdate {
    public long getUpdate(int columnId);

    public void setUpdate(int columnId, long update);

    public void incUpdate(int columnId, long update);

    public LongUpdateIterator iterator();
}
