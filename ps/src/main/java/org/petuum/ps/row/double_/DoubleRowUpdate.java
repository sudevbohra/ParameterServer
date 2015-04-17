package org.petuum.ps.row.double_;

import org.petuum.ps.row.RowUpdate;

/**
 * Created by aqiao on 2/20/15.
 */
public interface DoubleRowUpdate extends RowUpdate {
    public double getUpdate(int columnId);

    public void setUpdate(int columnId, double update);

    public void incUpdate(int columnId, double update);

    public DoubleUpdateIterator iterator();
}
