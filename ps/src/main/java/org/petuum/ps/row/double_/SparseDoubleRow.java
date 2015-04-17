package org.petuum.ps.row.double_;

import gnu.trove.iterator.TIntDoubleIterator;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import org.petuum.ps.row.Row;
import org.petuum.ps.row.RowUpdate;

import java.nio.ByteBuffer;

/**
 * Created by aqiao on 3/5/15.
 */
public class SparseDoubleRow extends DoubleRow {

    TIntDoubleMap params;

    SparseDoubleRow() {
        this.params = new TIntDoubleHashMap();
    }

    SparseDoubleRow(SparseDoubleRow other) {
        this.params = new TIntDoubleHashMap();
        this.params.putAll(other.params);
    }

    @Override
    public double getUnlocked(int columnId) {
        return this.params.get(columnId);
    }

    @Override
    public Row getCopyUnlocked() {
        return new SparseDoubleRow(this);
    }

    @Override
    public int getSerializedSizeUnlocked() {
        return (Integer.SIZE + params.size() * (Integer.SIZE + Double.SIZE)) / Byte.SIZE;
    }

    @Override
    public ByteBuffer serializeUnlocked() {
        ByteBuffer ret = ByteBuffer.allocate(this.getSerializedSizeUnlocked());
        ret.putInt(this.params.size());
        TIntDoubleIterator it = this.params.iterator();
        while (it.hasNext()) {
            it.advance();
            ret.putInt(it.key());
            ret.putDouble(it.value());
        }
        return ret;
    }

    @Override
    public void deserializeUnlocked(ByteBuffer data) {
        this.params.clear();
        int size = data.getInt();
        for (int i = 0; i < size; i++) {
            this.params.put(data.getInt(), data.getDouble());
        }
    }

    @Override
    public void resetUnlocked(Row row) {
        SparseDoubleRow sparseDoubleRow = (SparseDoubleRow) row;
        this.params.clear();
        this.params.putAll(sparseDoubleRow.params);
    }

    @Override
    public void applyRowUpdateUnlocked(RowUpdate rowUpdate) {
        DoubleRowUpdate doubleRowUpdate = (DoubleRowUpdate) rowUpdate;
        DoubleUpdateIterator it = doubleRowUpdate.iterator();
        while (it.hasNext()) {
            it.advance();
            this.params.adjustOrPutValue(it.getColumnId(), it.getUpdate(), it.getUpdate());
        }
    }
}
