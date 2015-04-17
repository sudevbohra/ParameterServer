package org.petuum.ps.row.double_;

import org.petuum.ps.row.AbstractRow;

/**
 * Created by aqiao on 2/23/15.
 */
public abstract class DoubleRow extends AbstractRow {

    public double get(int columnId) {
        this.acquireReadLock();
        try {
            return this.getUnlocked(columnId);
        } finally {
            this.releaseReadLock();
        }
    }

    public abstract double getUnlocked(int columnId);
}
