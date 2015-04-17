package org.petuum.ps.row.double_;

import org.petuum.ps.row.RowUpdate;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by aqiao on 2/20/15.
 */
public class DenseDoubleRowUpdate implements DoubleRowUpdate {

    private double[] updates;

    public DenseDoubleRowUpdate(int capacity) {
        this.updates = new double[capacity];
    }

    private DenseDoubleRowUpdate(DenseDoubleRowUpdate other) {
        this.updates = Arrays.copyOf(other.updates, other.updates.length);
    }

    DenseDoubleRowUpdate(ByteBuffer data) {
        int size = data.getInt();
        this.updates = new double[size];
        for (int i = 0; i < size; i++) {
            this.updates[i] = data.getDouble();
        }
    }

    @Override
    public double getUpdate(int columnId) {
        return this.updates[columnId];
    }

    @Override
    public void setUpdate(int columnId, double update) {
        this.updates[columnId] = update;
    }

    @Override
    public void incUpdate(int columnId, double update) {
        this.updates[columnId] += update;
    }

    @Override
    public DoubleUpdateIterator iterator() {
        return new DenseDoubleUpdateIterator();
    }

    @Override
    public RowUpdate getCopy() {
        return new DenseDoubleRowUpdate(this);
    }

    @Override
    public void addRowUpdate(RowUpdate rowUpdate) {
        DoubleRowUpdate doubleRowUpdate = (DoubleRowUpdate) rowUpdate;
        DoubleUpdateIterator it = doubleRowUpdate.iterator();
        while (it.hasNext()) {
            it.advance();
            this.updates[it.getColumnId()] += it.getUpdate();
        }
    }

    @Override
    public int getSerializedSize() {
        return Integer.SIZE / Byte.SIZE + this.updates.length * Double.SIZE / Byte.SIZE;
    }

    @Override
    public ByteBuffer serialize() {
        ByteBuffer ret = ByteBuffer.allocate(this.getSerializedSize());
        ret.putInt(this.updates.length);
        for (double d : this.updates) {
            ret.putDouble(d);
        }
        return ret;
    }

    @Override
    public void clear() {
        Arrays.fill(this.updates, 0.0);
    }

    private class DenseDoubleUpdateIterator implements DoubleUpdateIterator {

        private int columnId = -1;

        @Override
        public boolean hasNext() {
            return columnId < updates.length - 1;
        }

        @Override
        public void advance() {
            this.columnId++;
        }

        @Override
        public int getColumnId() {
            return this.columnId;
        }

        @Override
        public double getUpdate() {
            return updates[columnId];
        }
    }
}