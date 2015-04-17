package org.petuum.ps.row.int_;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import org.petuum.ps.row.Row;
import org.petuum.ps.row.RowUpdate;

import java.nio.ByteBuffer;

/**
 * Created by aqiao on 3/5/15.
 */
public class SparseIntRow extends IntRow {

    TIntIntMap params;

    SparseIntRow() {
        this.params = new TIntIntHashMap();
    }

    SparseIntRow(SparseIntRow other) {
        this.params = new TIntIntHashMap();
        this.params.putAll(other.params);
    }

    @Override
    public int getUnlocked(int columnId) {
        return this.params.get(columnId);
    }

    @Override
    public Row getCopyUnlocked() {
        return new SparseIntRow(this);
    }

    @Override
    public int getSerializedSizeUnlocked() {
        return (Integer.SIZE + params.size() * (Integer.SIZE + Integer.SIZE)) / Byte.SIZE;
    }

    @Override
    public ByteBuffer serializeUnlocked() {
        ByteBuffer ret = ByteBuffer.allocate(this.getSerializedSizeUnlocked());
        ret.putInt(this.params.size());
        TIntIntIterator it = this.params.iterator();
        while (it.hasNext()) {
            it.advance();
            ret.putInt(it.key());
            ret.putInt(it.value());
        }
        return ret;
    }

    @Override
    public void deserializeUnlocked(ByteBuffer data) {
        this.params.clear();
        int size = data.getInt();
        for (int i = 0; i < size; i++) {
            this.params.put(data.getInt(), data.getInt());
        }
    }

    @Override
    public void resetUnlocked(Row row) {
        SparseIntRow sparseIntRow = (SparseIntRow) row;
        this.params.clear();
        this.params.putAll(sparseIntRow.params);
    }

    @Override
    public void applyRowUpdateUnlocked(RowUpdate rowUpdate) {
        IntRowUpdate intRowUpdate = (IntRowUpdate) rowUpdate;
        IntUpdateIterator it = intRowUpdate.iterator();
        while (it.hasNext()) {
            it.advance();
            this.params.adjustOrPutValue(it.getColumnId(), it.getUpdate(), it.getUpdate());
        }
    }
}
