package org.petuum.ps.row.long_;

import gnu.trove.iterator.TIntLongIterator;
import gnu.trove.map.TIntLongMap;
import gnu.trove.map.hash.TIntLongHashMap;
import org.petuum.ps.row.Row;
import org.petuum.ps.row.RowUpdate;

import java.nio.ByteBuffer;

/**
 * Created by aqiao on 3/5/15.
 */
public class SparseLongRow extends LongRow {

    TIntLongMap params;

    SparseLongRow() {
        this.params = new TIntLongHashMap();
    }

    SparseLongRow(SparseLongRow other) {
        this.params = new TIntLongHashMap();
        this.params.putAll(other.params);
    }

    @Override
    public long getUnlocked(int columnId) {
        return this.params.get(columnId);
    }

    @Override
    public Row getCopyUnlocked() {
        return new SparseLongRow(this);
    }

    @Override
    public int getSerializedSizeUnlocked() {
        return (Integer.SIZE + params.size() * (Integer.SIZE + Long.SIZE)) / Byte.SIZE;
    }

    @Override
    public ByteBuffer serializeUnlocked() {
        ByteBuffer ret = ByteBuffer.allocate(this.getSerializedSizeUnlocked());
        ret.putInt(this.params.size());
        TIntLongIterator it = this.params.iterator();
        while (it.hasNext()) {
            it.advance();
            ret.putInt(it.key());
            ret.putLong(it.value());
        }
        return ret;
    }

    @Override
    public void deserializeUnlocked(ByteBuffer data) {
        this.params.clear();
        int size = data.getInt();
        for (int i = 0; i < size; i++) {
            this.params.put(data.getInt(), data.getLong());
        }
    }

    @Override
    public void resetUnlocked(Row row) {
        SparseLongRow sparseLongRow = (SparseLongRow) row;
        this.params.clear();
        this.params.putAll(sparseLongRow.params);
    }

    @Override
    public void applyRowUpdateUnlocked(RowUpdate rowUpdate) {
        LongRowUpdate longRowUpdate = (LongRowUpdate) rowUpdate;
        LongUpdateIterator it = longRowUpdate.iterator();
        while (it.hasNext()) {
            it.advance();
            this.params.adjustOrPutValue(it.getColumnId(), it.getUpdate(), it.getUpdate());
        }
    }
}
