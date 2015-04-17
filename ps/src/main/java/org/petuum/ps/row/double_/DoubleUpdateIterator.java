package org.petuum.ps.row.double_;

/**
 * Created by aqiao on 2/23/15.
 */
public interface DoubleUpdateIterator {
    public boolean hasNext();

    public void advance();

    public int getColumnId();

    public double getUpdate();
}
