package org.petuum.ps.row;

import java.nio.ByteBuffer;

/**
 * Created by aqiao on 2/23/15.
 */
public interface Row {
    public Row getCopy();

    public Row getCopyUnlocked();

    public int getSerializedSize();

    public int getSerializedSizeUnlocked();

    public ByteBuffer serialize();

    public ByteBuffer serializeUnlocked();

    public void deserialize(ByteBuffer data);

    public void deserializeUnlocked(ByteBuffer data);

    public void reset(Row row);

    public void resetUnlocked(Row row);

    public void applyRowUpdate(RowUpdate rowUpdate);

    public void applyRowUpdateUnlocked(RowUpdate rowUpdate);

    public void acquireReadLock();

    public void releaseReadLock();

    public void acquireWriteLock();

    public void releaseWriteLock();
}
