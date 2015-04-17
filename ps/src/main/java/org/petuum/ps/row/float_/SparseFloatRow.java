package org.petuum.ps.row.float_;

import gnu.trove.iterator.TIntFloatIterator;
import gnu.trove.map.TIntFloatMap;
import gnu.trove.map.hash.TIntFloatHashMap;
import org.petuum.ps.row.Row;
import org.petuum.ps.row.RowUpdate;

import java.nio.ByteBuffer;

/**
 * Created by aqiao on 3/5/15.
 */
public class SparseFloatRow extends FloatRow {

    TIntFloatMap params;

    SparseFloatRow() {
        this.params = new TIntFloatHashMap();
    }

    SparseFloatRow(SparseFloatRow other) {
        this.params = new TIntFloatHashMap();
        this.params.putAll(other.params);
    }

    @Override
    public float getUnlocked(int columnId) {
        return this.params.get(columnId);
    }

    @Override
    public Row getCopyUnlocked() {
        return new SparseFloatRow(this);
    }

    @Override
    public int getSerializedSizeUnlocked() {
        return (Integer.SIZE + params.size() * (Integer.SIZE + Float.SIZE)) / Byte.SIZE;
    }

    @Override
    public ByteBuffer serializeUnlocked() {
        ByteBuffer ret = ByteBuffer.allocate(this.getSerializedSizeUnlocked());
        ret.putInt(this.params.size());
        TIntFloatIterator it = this.params.iterator();
        while (it.hasNext()) {
            it.advance();
            ret.putInt(it.key());
            ret.putFloat(it.value());
        }
        return ret;
    }

    @Override
    public void deserializeUnlocked(ByteBuffer data) {
        this.params.clear();
        int size = data.getInt();
        for (int i = 0; i < size; i++) {
            this.params.put(data.getInt(), data.getFloat());
        }
    }

    @Override
    public void resetUnlocked(Row row) {
        SparseFloatRow sparseFloatRow = (SparseFloatRow) row;
        this.params.clear();
        this.params.putAll(sparseFloatRow.params);
    }

    @Override
    public void applyRowUpdateUnlocked(RowUpdate rowUpdate) {
        FloatRowUpdate floatRowUpdate = (FloatRowUpdate) rowUpdate;
        FloatUpdateIterator it = floatRowUpdate.iterator();
        while (it.hasNext()) {
            it.advance();
            this.params.adjustOrPutValue(it.getColumnId(), it.getUpdate(), it.getUpdate());
        }
    }
}
