package org.petuum.ps.row.int_;

/**
 * Created by aqiao on 2/23/15.
 */
public interface IntUpdateIterator {
    public boolean hasNext();

    public void advance();

    public int getColumnId();

    public int getUpdate();
}
