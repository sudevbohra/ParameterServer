package org.petuum.ps.row.int_;

import org.petuum.ps.row.RowUpdate;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by aqiao on 2/20/15.
 */
public class DenseIntRowUpdate implements IntRowUpdate {

    private int[] updates;

    public DenseIntRowUpdate(int capacity) {
        this.updates = new int[capacity];
    }

    private DenseIntRowUpdate(DenseIntRowUpdate other) {
        this.updates = Arrays.copyOf(other.updates, other.updates.length);
    }

    DenseIntRowUpdate(ByteBuffer data) {
        int size = data.getInt();
        this.updates = new int[size];
        for (int i = 0; i < size; i++) {
            this.updates[i] = data.getInt();
        }
    }

    @Override
    public int getUpdate(int columnId) {
        return this.updates[columnId];
    }

    @Override
    public void setUpdate(int columnId, int update) {
        this.updates[columnId] = update;
    }

    @Override
    public void incUpdate(int columnId, int update) {
        this.updates[columnId] += update;
    }

    @Override
    public IntUpdateIterator iterator() {
        return new DenseIntUpdateIterator();
    }

    @Override
    public RowUpdate getCopy() {
        return new DenseIntRowUpdate(this);
    }

    @Override
    public void addRowUpdate(RowUpdate rowUpdate) {
        IntRowUpdate intRowUpdate = (IntRowUpdate) rowUpdate;
        IntUpdateIterator it = intRowUpdate.iterator();
        while (it.hasNext()) {
            it.advance();
            this.updates[it.getColumnId()] += it.getUpdate();
        }
    }

    @Override
    public int getSerializedSize() {
        return Integer.SIZE / Byte.SIZE + this.updates.length * Integer.SIZE / Byte.SIZE;
    }

    @Override
    public ByteBuffer serialize() {
        ByteBuffer ret = ByteBuffer.allocate(this.getSerializedSize());
        ret.putInt(this.updates.length);
        for (int d : this.updates) {
            ret.putInt(d);
        }
        return ret;
    }

    @Override
    public void clear() {
        Arrays.fill(this.updates, 0);
    }

    private class DenseIntUpdateIterator implements IntUpdateIterator {

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
        public int getUpdate() {
            return updates[columnId];
        }
    }
}