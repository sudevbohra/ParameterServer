package org.petuum.ps.client.oplog;

import com.google.common.util.concurrent.Striped;
import gnu.trove.TCollections;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.petuum.ps.client.thread.GlobalContext;
import org.petuum.ps.config.Config;
import org.petuum.ps.row.RowUpdate;
import org.petuum.ps.row.RowUpdateFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by aqiao on 3/17/15.
 */
public class SparseOplogPartition implements OplogPartition {

    private ArrayList<TIntObjectMap<RowUpdate>> rowUpdateMap;
    private ArrayList<ReadWriteLock> locks;
    private RowUpdateFactory rowUpdateFactory;
    private Config rowUpdateConfig;

    public SparseOplogPartition(RowUpdateFactory rowUpdateFactory, Config rowUpdateConfig) {
        rowUpdateMap = new ArrayList<TIntObjectMap<RowUpdate>>();
        locks = new ArrayList<ReadWriteLock>();
        for (int i = 0; i < GlobalContext.getLockPoolSize(); i ++) {
            rowUpdateMap.add(new TIntObjectHashMap<RowUpdate>());
            locks.add(new ReentrantReadWriteLock());
        }
        this.rowUpdateFactory = rowUpdateFactory;
        this.rowUpdateConfig = rowUpdateConfig;
    }

    @Override
    public RowUpdate createRowUpdate(int rowId) {
        RowUpdate rowOplog = rowUpdateFactory.create(this.rowUpdateConfig);
        locks.get(rowId % rowUpdateMap.size()).writeLock().lock();
        try {
            rowUpdateMap.get(rowId % rowUpdateMap.size()).put(rowId, rowOplog);
            return rowOplog;
        } finally {
            locks.get(rowId % rowUpdateMap.size()).writeLock().unlock();
        }
    }

    @Override
    public RowUpdate getRowUpdate(int rowId) {
        locks.get(rowId % rowUpdateMap.size()).readLock().lock();
        try {
            return rowUpdateMap.get(rowId % rowUpdateMap.size()).get(rowId);
        } finally {
            locks.get(rowId % rowUpdateMap.size()).readLock().unlock();
        }
    }

    @Override
    public RowUpdate removeRowUpdate(int rowId) {
        locks.get(rowId % rowUpdateMap.size()).writeLock().lock();
        try {
            return rowUpdateMap.get(rowId % rowUpdateMap.size()).remove(rowId);
        } finally {
            locks.get(rowId % rowUpdateMap.size()).writeLock().unlock();
        }
    }

    /*public void serializeByServer(TIntObjectMap<ByteBuffer> bytesByServer) {

        TIntIntMap offsetByServer = new TIntIntHashMap();

        for (TIntObjectIterator<ByteBuffer> iter = bytesByServer.iterator(); iter
                .hasNext(); ) {
            iter.advance();
            int serverId = iter.key();

            offsetByServer.put(serverId, Integer.SIZE / Byte.SIZE);
            // init number of rows to 0
            iter.value().putInt(0, 0);
        }

        for (TIntObjectIterator<RowUpdate> iter = this.rowUpdateMap
                .iterator(); iter.hasNext(); ) {
            iter.advance();
            int rowId = iter.key();
            int serverId = GlobalContext.getPartitionServerId(rowId,
                    this.commChannelIdx);

            ByteBuffer mem = bytesByServer.get(serverId);

            RowUpdate rowOplogPtr = iter.value();

            mem.position(offsetByServer.get(serverId));
            mem.putInt(rowId);

            ByteBuffer serialized = rowOplogPtr.serialize();
            serialized.rewind();
            int serializedSize = serialized.capacity();
            mem.put(serialized);

            offsetByServer.put(serverId, offsetByServer.get(serverId)
                    + Integer.SIZE / Byte.SIZE + serializedSize);

            mem.putInt(0, mem.getInt(0) + 1);
        }
    }*/
}
