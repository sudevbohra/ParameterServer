package org.petuum.ps.client.thread;

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.petuum.ps.row.RowUpdate;

import java.nio.ByteBuffer;

public class BgOpLogPartition {
    private TIntObjectMap<RowUpdate> oplogMap;
    private int tableId;
    private int commChannelIdx;

    public BgOpLogPartition(int tableId, int myCommChannelIdx) {
        this.tableId = tableId;
        this.commChannelIdx = myCommChannelIdx;
        this.oplogMap = new TIntObjectHashMap<>();
    }

    public RowUpdate findOpLog(int rowId) {
        if (oplogMap.containsKey(rowId)) {
            return oplogMap.get(rowId);
        } else {
            return null;
        }
    }

    public void insertOpLog(int rowId, RowUpdate rowOplog) {
        oplogMap.put(rowId, rowOplog);
    }

    public void serializeByServer(TIntObjectMap<ByteBuffer> bytesByServer) {

        TIntIntMap offsetByServer = new TIntIntHashMap();

        for (TIntObjectIterator<ByteBuffer> iter = bytesByServer.iterator(); iter
                .hasNext(); ) {
            iter.advance();
            int serverId = iter.key();

            offsetByServer.put(serverId, Integer.SIZE / Byte.SIZE);
            // init number of rows to 0
            iter.value().putInt(0, 0);
        }

        for (TIntObjectIterator<RowUpdate> iter = this.oplogMap
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
    }
}
