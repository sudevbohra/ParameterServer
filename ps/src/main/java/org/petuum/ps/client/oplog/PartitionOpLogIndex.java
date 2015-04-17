package org.petuum.ps.client.oplog;

import com.google.common.util.concurrent.Striped;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import gnu.trove.set.TIntSet;
import org.petuum.ps.client.thread.GlobalContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PartitionOpLogIndex {
    private int capacity;
    private ReadWriteLock sharedLock;
    private Striped<Lock> locks;
    //private TIntObjectMap<Boolean> sharedOpLogIndex;
    private Map<Integer, Boolean> sharedOpLogIndex;
    public PartitionOpLogIndex(int capacity) {
        this.capacity = capacity;
        this.locks = Striped.lock(GlobalContext.getLockPoolSize());
        this.sharedLock = new ReentrantReadWriteLock();
        //this.sharedOpLogIndex = new TIntObjectHashMap<>();
        this.sharedOpLogIndex = new HashMap<>();
    }

    public void addIndex(TIntSet opLogIndex) {
        try {
            sharedLock.readLock().lock();

            TIntIterator iter = opLogIndex.iterator();
            while (iter.hasNext()) {
                int index = iter.next();
                Lock lock = locks.get(index);
                try {
                    lock.lock();
                    sharedOpLogIndex.put(index, true);
                } finally {
                    lock.unlock();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sharedLock.readLock().unlock();
        }
    }

    public Map<Integer, Boolean> reset() {
        Map<Integer, Boolean> oldIndex;
        sharedLock.writeLock().lock();
        try {
            oldIndex = sharedOpLogIndex;
            sharedOpLogIndex = new HashMap<>();
        } finally {
            sharedLock.writeLock().unlock();
        }
        return oldIndex;
    }

    public int getNumRowOpLogs() {
        sharedLock.readLock().lock();
        try {
            return sharedOpLogIndex.size();
        } finally {
            sharedLock.readLock().unlock();
        }
    }
}
