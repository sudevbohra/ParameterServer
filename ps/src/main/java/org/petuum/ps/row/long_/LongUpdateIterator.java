package org.petuum.ps.row.long_;

/**
 * Created by aqiao on 2/23/15.
 */
public interface LongUpdateIterator {
    public boolean hasNext();

    public void advance();

    public int getColumnId();

    public long getUpdate();
}
