package org.petuum.ps.row.float_;

import org.petuum.ps.row.RowUpdate;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by aqiao on 2/20/15.
 */
public class DenseFloatRowUpdate implements FloatRowUpdate {

    private float[] updates;

    public DenseFloatRowUpdate(int capacity) {
        this.updates = new float[capacity];
    }

    private DenseFloatRowUpdate(DenseFloatRowUpdate other) {
        this.updates = Arrays.copyOf(other.updates, other.updates.length);
    }

    DenseFloatRowUpdate(ByteBuffer data) {
        int size = data.getInt();
        this.updates = new float[size];
        for (int i = 0; i < size; i++) {
            this.updates[i] = data.getFloat();
        }
    }

    @Override
    public float getUpdate(int columnId) {
        return this.updates[columnId];
    }

    @Override
    public void setUpdate(int columnId, float update) {
        this.updates[columnId] = update;
    }

    @Override
    public void incUpdate(int columnId, float update) {
        this.updates[columnId] += update;
    }

    @Override
    public FloatUpdateIterator iterator() {
        return new DenseFloatUpdateIterator();
    }

    @Override
    public RowUpdate getCopy() {
        return new DenseFloatRowUpdate(this);
    }

    @Override
    public void addRowUpdate(RowUpdate rowUpdate) {
        FloatRowUpdate floatRowUpdate = (FloatRowUpdate) rowUpdate;
        FloatUpdateIterator it = floatRowUpdate.iterator();
        while (it.hasNext()) {
            it.advance();
            this.updates[it.getColumnId()] += it.getUpdate();
        }
    }

    @Override
    public int getSerializedSize() {
        return Integer.SIZE / Byte.SIZE + this.updates.length * Float.SIZE / Byte.SIZE;
    }

    @Override
    public ByteBuffer serialize() {
        ByteBuffer ret = ByteBuffer.allocate(this.getSerializedSize());
        ret.putInt(this.updates.length);
        for (float d : this.updates) {
            ret.putFloat(d);
        }
        return ret;
    }

    @Override
    public void clear() {
        Arrays.fill(this.updates, 0);
    }

    private class DenseFloatUpdateIterator implements FloatUpdateIterator {

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
        public float getUpdate() {
            return updates[columnId];
        }
    }
}