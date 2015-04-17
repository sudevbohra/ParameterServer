package org.petuum.ps.row.float_;

import org.petuum.ps.row.RowUpdate;

/**
 * Created by aqiao on 2/20/15.
 */
public interface FloatRowUpdate extends RowUpdate {
    public float getUpdate(int columnId);

    public void setUpdate(int columnId, float update);

    public void incUpdate(int columnId, float update);

    public FloatUpdateIterator iterator();
}
