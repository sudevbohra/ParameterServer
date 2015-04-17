package org.petuum.ps.row;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by aqiao on 3/2/15.
 */
public abstract class AbstractRow implements Row {

    private ReadWriteLock rwLock;

    public AbstractRow() {
        this.rwLock = new ReentrantReadWriteLock();
    }

    @Override
    public Row getCopy() {
        this.rwLock.readLock().lock();
        try {
            return this.getCopyUnlocked();
        } finally {
            this.rwLock.readLock().unlock();
        }
    }

    @Override
    public int getSerializedSize() {
        this.rwLock.readLock().lock();
        try {
            return this.getSerializedSizeUnlocked();
        } finally {
            this.rwLock.readLock().unlock();
        }
    }

    @Override
    public ByteBuffer serialize() {
        this.rwLock.readLock().lock();
        try {
            return this.serializeUnlocked();
        } finally {
            this.rwLock.readLock().unlock();
        }
    }

    @Override
    public void deserialize(ByteBuffer data) {
        this.rwLock.writeLock().lock();
        try {
            this.deserializeUnlocked(data);
        } finally {
            this.rwLock.writeLock().unlock();
        }
    }

    @Override
    public void reset(Row row) {
        this.rwLock.writeLock().lock();
        try {
            this.resetUnlocked(row);
        } finally {
            this.rwLock.writeLock().unlock();
        }
    }

    @Override
    public void applyRowUpdate(RowUpdate rowUpdate) {
        this.rwLock.writeLock().lock();
        try {
            this.applyRowUpdateUnlocked(rowUpdate);
        } finally {
            this.rwLock.writeLock().unlock();
        }
    }

    @Override
    public void acquireReadLock() {
        this.rwLock.readLock().lock();
    }

    @Override
    public void releaseReadLock() {
        this.rwLock.readLock().unlock();
    }

    @Override
    public void acquireWriteLock() {
        this.rwLock.writeLock().lock();
    }

    @Override
    public void releaseWriteLock() {
        this.rwLock.writeLock().unlock();
    }
}
