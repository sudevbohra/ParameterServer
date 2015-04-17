package org.petuum.ps.row.int_;

import org.petuum.ps.row.AbstractRow;

/**
 * Created by aqiao on 2/23/15.
 */
public abstract class IntRow extends AbstractRow {

    public int get(int columnId) {
        this.acquireReadLock();
        try {
            return this.getUnlocked(columnId);
        } finally {
            this.releaseReadLock();
        }
    }

    public abstract int getUnlocked(int columnId);
}
