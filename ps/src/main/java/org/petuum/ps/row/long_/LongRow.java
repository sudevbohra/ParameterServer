package org.petuum.ps.row.long_;

import org.petuum.ps.row.AbstractRow;

/**
 * Created by aqiao on 2/23/15.
 */
public abstract class LongRow extends AbstractRow {

    public long get(int columnId) {
        this.acquireReadLock();
        try {
            return this.getUnlocked(columnId);
        } finally {
            this.releaseReadLock();
        }
    }

    public abstract long getUnlocked(int columnId);
}
