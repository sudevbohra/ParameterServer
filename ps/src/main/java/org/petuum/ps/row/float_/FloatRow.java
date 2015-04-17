package org.petuum.ps.row.float_;

import org.petuum.ps.row.AbstractRow;

/**
 * Created by aqiao on 2/23/15.
 */
public abstract class FloatRow extends AbstractRow {

    public float get(int columnId) {
        this.acquireReadLock();
        try {
            return this.getUnlocked(columnId);
        } finally {
            this.releaseReadLock();
        }
    }

    public abstract float getUnlocked(int columnId);
}
