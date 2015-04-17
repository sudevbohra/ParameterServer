package org.petuum.ps.client.thread;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

public class BgOpLog {
    private TIntObjectMap<BgOpLogPartition> tableOplogMap;

    public BgOpLog() {
        tableOplogMap = new TIntObjectHashMap<>();
    }

    public void Add(int tableId, BgOpLogPartition bgOplogPartitionPtr) {
        tableOplogMap.put(tableId, bgOplogPartitionPtr);
    }

    public BgOpLogPartition get(int tableId) {
        return tableOplogMap.get(tableId);
    }
}
