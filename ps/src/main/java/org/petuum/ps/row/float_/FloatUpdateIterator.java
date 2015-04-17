package org.petuum.ps.row.float_;

/**
 * Created by aqiao on 2/23/15.
 */
public interface FloatUpdateIterator {
    public boolean hasNext();

    public void advance();

    public int getColumnId();

    public float getUpdate();
}
