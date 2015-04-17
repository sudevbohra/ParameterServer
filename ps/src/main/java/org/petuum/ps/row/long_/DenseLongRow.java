package org.petuum.ps.row.long_;

import org.petuum.ps.row.Row;
import org.petuum.ps.row.RowUpdate;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by aqiao on 2/23/15.
 */
public class DenseLongRow extends LongRow {

    private long[] params;

    public DenseLongRow(int capacity) {
        this.params = new long[capacity];
    }

    DenseLongRow(DenseLongRow other) {
        this.params = Arrays.copyOf(other.params, other.params.length);
    }

    @Override
    public long getUnlocked(int columnId) {
        return params[columnId];
    }

    @Override
    public Row getCopyUnlocked() {
        return new DenseLongRow(this);
    }

    @Override
    public int getSerializedSizeUnlocked() {
        return (Integer.SIZE + this.params.length * Long.SIZE) / Byte.SIZE;
    }

    @Override
    public ByteBuffer serializeUnlocked() {
        ByteBuffer ret = ByteBuffer.allocate(this.getSerializedSizeUnlocked());
        ret.putInt(this.params.length);
        for (long d : params) {
            ret.putLong(d);
        }
        return ret;
    }

    @Override
    public void deserializeUnlocked(ByteBuffer data) {
        int size = data.getInt();
        assert (size == this.params.length);
        for (int i = 0; i < size; i++) {
            this.params[i] = data.getLong();
        }
    }

    @Override
    public void resetUnlocked(Row row) {
        DenseLongRow denseLongRow = (DenseLongRow) row;
        this.params = Arrays.copyOf(denseLongRow.params, denseLongRow.params.length);
    }

    @Override
    public void applyRowUpdateUnlocked(RowUpdate rowUpdate) {
        LongRowUpdate longRowUpdate = (LongRowUpdate) rowUpdate;
        LongUpdateIterator it = longRowUpdate.iterator();
        while (it.hasNext()) {
            it.advance();
            this.params[it.getColumnId()] += it.getUpdate();
        }
    }
}
