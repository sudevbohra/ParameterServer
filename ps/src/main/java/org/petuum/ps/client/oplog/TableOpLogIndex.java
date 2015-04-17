package org.petuum.ps.client.oplog;

import gnu.trove.map.TIntObjectMap;

import gnu.trove.set.TIntSet;
import org.petuum.ps.client.thread.GlobalContext;

import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class TableOpLogIndex {
    private Vector<PartitionOpLogIndex> partitionOplogIndex;

    public TableOpLogIndex(int capacity) {
        partitionOplogIndex = new Vector<>();
        for (int i = 0; i < GlobalContext.getNumCommChannelsPerClient(); i++)
            partitionOplogIndex.add(new PartitionOpLogIndex(capacity));
    }

    public void addIndex(int partitionNum, TIntSet oplogIndex) {
        partitionOplogIndex.get(partitionNum).addIndex(oplogIndex);
    }

    public Map<Integer, Boolean> resetPartition(int partitionNum) {
        return partitionOplogIndex.get(partitionNum).reset();
    }

    public int getNumRowOpLogs(int partitionNum) {
        return partitionOplogIndex.get(partitionNum).getNumRowOpLogs();
    }
}
