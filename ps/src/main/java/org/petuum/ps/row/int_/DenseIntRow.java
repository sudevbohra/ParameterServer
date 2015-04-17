package org.petuum.ps.row.int_;

import org.petuum.ps.row.Row;
import org.petuum.ps.row.RowUpdate;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by aqiao on 2/23/15.
 */
public class DenseIntRow extends IntRow {

    private int[] params;

    public DenseIntRow(int capacity) {
        this.params = new int[capacity];
    }

    DenseIntRow(DenseIntRow other) {
        this.params = Arrays.copyOf(other.params, other.params.length);
    }

    @Override
    public int getUnlocked(int columnId) {
        return params[columnId];
    }

    @Override
    public Row getCopyUnlocked() {
        return new DenseIntRow(this);
    }

    @Override
    public int getSerializedSizeUnlocked() {
        return (Integer.SIZE + this.params.length * Integer.SIZE) / Byte.SIZE;
    }

    @Override
    public ByteBuffer serializeUnlocked() {
        ByteBuffer ret = ByteBuffer.allocate(this.getSerializedSizeUnlocked());
        ret.putInt(this.params.length);
        for (int d : params) {
            ret.putInt(d);
        }
        return ret;
    }

    @Override
    public void deserializeUnlocked(ByteBuffer data) {
        int size = data.getInt();
        assert (size == this.params.length);
        for (int i = 0; i < size; i++) {
            this.params[i] = data.getInt();
        }
    }

    @Override
    public void resetUnlocked(Row row) {
        DenseIntRow denseIntRow = (DenseIntRow) row;
        this.params = Arrays.copyOf(denseIntRow.params, denseIntRow.params.length);
    }

    @Override
    public void applyRowUpdateUnlocked(RowUpdate rowUpdate) {
        IntRowUpdate intRowUpdate = (IntRowUpdate) rowUpdate;
        IntUpdateIterator it = intRowUpdate.iterator();
        while (it.hasNext()) {
            it.advance();
            this.params[it.getColumnId()] += it.getUpdate();
        }
    }
}
