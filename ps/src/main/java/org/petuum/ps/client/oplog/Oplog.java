package org.petuum.ps.client.oplog;

import com.google.common.util.concurrent.Striped;
import gnu.trove.TCollections;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.petuum.ps.client.thread.GlobalContext;
import org.petuum.ps.config.Config;
import org.petuum.ps.row.RowUpdate;
import org.petuum.ps.row.RowUpdateFactory;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;

/**
 * Created by aqiao on 3/17/15.
 */
public class Oplog {
    private Striped<Lock> locks;
    private ArrayList<TIntObjectHashMap<OplogPartition>> oplogPartitionMaps;

    public Oplog(RowUpdateFactory rowUpdateFactory, Config rowUpdateConfig) {
        this.locks = Striped.lock(GlobalContext.getLockPoolSize());
        this.oplogPartitionMaps = new ArrayList<TIntObjectHashMap<OplogPartition>>();
        for (int i = 0; i < GlobalContext.getNumCommChannelsPerClient(); i ++) {
            this.oplogPartitionMaps.add(new TIntObjectHashMap<OplogPartition>());
            ArrayList<Integer> serverIds = GlobalContext.getServerThreadIDs(i);
            for (int serverId : serverIds) {
                this.oplogPartitionMaps.get(i).put(serverId,
                        new SparseOplogPartition(rowUpdateFactory, rowUpdateConfig));
            }
        }
    }

    public void lockRow(int rowId) {
        this.locks.get(rowId).lock();
    }

    public void unlockRow(int rowId) {
        this.locks.get(rowId).unlock();
    }

    public RowUpdate createRowUpdate(int rowId) {
        int idx = GlobalContext.getPartitionCommChannelIndex(rowId);
        int serverId = GlobalContext.getPartitionServerId(rowId, idx);
        return this.oplogPartitionMaps.get(idx).get(serverId).createRowUpdate(rowId);
    }

    public RowUpdate getRowUpdate(int rowId) {
        int idx = GlobalContext.getPartitionCommChannelIndex(rowId);
        int serverId = GlobalContext.getPartitionServerId(rowId, idx);
        return this.oplogPartitionMaps.get(idx).get(serverId).getRowUpdate(rowId);
    }

    public RowUpdate removeRowUpdate(int rowId) {
        int idx = GlobalContext.getPartitionCommChannelIndex(rowId);
        int serverId = GlobalContext.getPartitionServerId(rowId, idx);
        return this.oplogPartitionMaps.get(idx).get(serverId).removeRowUpdate(rowId);
    }
}
