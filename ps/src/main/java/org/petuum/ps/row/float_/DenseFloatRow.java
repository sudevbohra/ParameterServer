package org.petuum.ps.row.float_;

import org.petuum.ps.row.Row;
import org.petuum.ps.row.RowUpdate;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by aqiao on 2/23/15.
 */
public class DenseFloatRow extends FloatRow {

    private float[] params;

    public DenseFloatRow(int capacity) {
        this.params = new float[capacity];
    }

    DenseFloatRow(DenseFloatRow other) {
        this.params = Arrays.copyOf(other.params, other.params.length);
    }

    @Override
    public float getUnlocked(int columnId) {
        return params[columnId];
    }

    @Override
    public Row getCopyUnlocked() {
        return new DenseFloatRow(this);
    }

    @Override
    public int getSerializedSizeUnlocked() {
        return (Integer.SIZE + this.params.length * Float.SIZE) / Byte.SIZE;
    }

    @Override
    public ByteBuffer serializeUnlocked() {
        ByteBuffer ret = ByteBuffer.allocate(this.getSerializedSizeUnlocked());
        ret.putInt(this.params.length);
        for (float d : params) {
            ret.putFloat(d);
        }
        return ret;
    }

    @Override
    public void deserializeUnlocked(ByteBuffer data) {
        int size = data.getInt();
        assert (size == this.params.length);
        for (int i = 0; i < size; i++) {
            this.params[i] = data.getFloat();
        }
    }

    @Override
    public void resetUnlocked(Row row) {
        DenseFloatRow denseFloatRow = (DenseFloatRow) row;
        this.params = Arrays.copyOf(denseFloatRow.params, denseFloatRow.params.length);
    }

    @Override
    public void applyRowUpdateUnlocked(RowUpdate rowUpdate) {
        FloatRowUpdate intRowUpdate = (FloatRowUpdate) rowUpdate;
        FloatUpdateIterator it = intRowUpdate.iterator();
        while (it.hasNext()) {
            it.advance();
            this.params[it.getColumnId()] += it.getUpdate();
        }
    }
}
