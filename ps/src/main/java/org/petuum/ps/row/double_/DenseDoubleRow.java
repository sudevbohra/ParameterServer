package org.petuum.ps.row.double_;

import org.petuum.ps.row.Row;
import org.petuum.ps.row.RowUpdate;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by aqiao on 2/23/15.
 */
public class DenseDoubleRow extends DoubleRow {

    private double[] params;

    public DenseDoubleRow(int capacity) {
        this.params = new double[capacity];
    }

    DenseDoubleRow(DenseDoubleRow other) {
        this.params = Arrays.copyOf(other.params, other.params.length);
    }

    @Override
    public double getUnlocked(int columnId) {
        return params[columnId];
    }

    @Override
    public Row getCopyUnlocked() {
        return new DenseDoubleRow(this);
    }

    @Override
    public int getSerializedSizeUnlocked() {
        return (Integer.SIZE + this.params.length * Double.SIZE) / Byte.SIZE;
    }

    @Override
    public ByteBuffer serializeUnlocked() {
        ByteBuffer ret = ByteBuffer.allocate(this.getSerializedSizeUnlocked());
        ret.putInt(this.params.length);
        for (double d : params) {
            ret.putDouble(d);
        }
        return ret;
    }

    @Override
    public void deserializeUnlocked(ByteBuffer data) {
        int size = data.getInt();
        assert (size == this.params.length);
        for (int i = 0; i < size; i++) {
            this.params[i] = data.getDouble();
        }
    }

    @Override
    public void resetUnlocked(Row row) {
        DenseDoubleRow denseDoubleRow = (DenseDoubleRow) row;
        this.params = Arrays.copyOf(denseDoubleRow.params, denseDoubleRow.params.length);
    }

    @Override
    public void applyRowUpdateUnlocked(RowUpdate rowUpdate) {
        DoubleRowUpdate doubleRowUpdate = (DoubleRowUpdate) rowUpdate;
        DoubleUpdateIterator it = doubleRowUpdate.iterator();
        while (it.hasNext()) {
            it.advance();
            this.params[it.getColumnId()] += it.getUpdate();
        }
    }
}
