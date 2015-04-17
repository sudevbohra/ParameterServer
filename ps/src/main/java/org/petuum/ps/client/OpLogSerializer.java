package org.petuum.ps.client;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

import java.nio.ByteBuffer;

public class OpLogSerializer {

    private TIntIntMap offsetMap;
    private ByteBuffer mem;
    private int numTables;

    public OpLogSerializer() {
        this.offsetMap = new TIntIntHashMap();
    }

    public int init(TIntIntMap tableSizeMap) {
        numTables = tableSizeMap.size();
        if (numTables == 0)
            return 0;

        // space for num of tables
        int totalSize = Integer.SIZE / Byte.SIZE;
        for (TIntIntIterator iter = tableSizeMap.iterator(); iter.hasNext(); ) {
            iter.advance();
            int tableId = iter.key();
            int tableSize = iter.value();
            offsetMap.put(tableId, totalSize);
            // next table is offset by
            // 1) the current table size and
            // 2) space for table id
            // 3) update size
            totalSize += tableSize + Integer.SIZE / Byte.SIZE + Integer.SIZE
                    / Byte.SIZE;
        }
        return totalSize;
    }

    // does not take ownership
    public void assignMem(ByteBuffer mem) {
        this.mem = mem;
        this.mem.putInt(0, numTables);
    }

    public ByteBuffer getTablePtr(int tableId) {
        Integer offset = null;
        if (offsetMap.containsKey(tableId)) {
            offset = offsetMap.get(tableId);
        }
        if (offset == null) {
            return null;
        }
        mem.position(offset);
        return mem.slice();
    }

}
