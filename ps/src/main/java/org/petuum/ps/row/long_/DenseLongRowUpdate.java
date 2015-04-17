package org.petuum.ps.row.long_;

import org.petuum.ps.row.RowUpdate;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by aqiao on 2/20/15.
 */
public class DenseLongRowUpdate implements LongRowUpdate {

    private long[] updates;

    public DenseLongRowUpdate(int capacity) {
        this.updates = new long[capacity];
    }

    private DenseLongRowUpdate(DenseLongRowUpdate other) {
        this.updates = Arrays.copyOf(other.updates, other.updates.length);
    }

    DenseLongRowUpdate(ByteBuffer data) {
        int size = data.getInt();
        this.updates = new long[size];
        for (int i = 0; i < size; i++) {
            this.updates[i] = data.getLong();
        }
    }

    @Override
    public long getUpdate(int columnId) {
        return this.updates[columnId];
    }

    @Override
    public void setUpdate(int columnId, long update) {
        this.updates[columnId] = update;
    }

    @Override
    public void incUpdate(int columnId, long update) {
        this.updates[columnId] += update;
    }

    @Override
    public LongUpdateIterator iterator() {
        return new DenseLongUpdateIterator();
    }

    @Override
    public RowUpdate getCopy() {
        return new DenseLongRowUpdate(this);
    }

    @Override
    public void addRowUpdate(RowUpdate rowUpdate) {
        LongRowUpdate longRowUpdate = (LongRowUpdate) rowUpdate;
        LongUpdateIterator it = longRowUpdate.iterator();
        while (it.hasNext()) {
            it.advance();
            this.updates[it.getColumnId()] += it.getUpdate();
        }
    }

    @Override
    public int getSerializedSize() {
        return Integer.SIZE / Byte.SIZE + this.updates.length * Long.SIZE / Byte.SIZE;
    }

    @Override
    public ByteBuffer serialize() {
        ByteBuffer ret = ByteBuffer.allocate(this.getSerializedSize());
        ret.putInt(this.updates.length);
        for (long d : this.updates) {
            ret.putLong(d);
        }
        return ret;
    }

    @Override
    public void clear() {
        Arrays.fill(this.updates, 0);
    }

    private class DenseLongUpdateIterator implements LongUpdateIterator {

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
        public long getUpdate() {
            return updates[columnId];
        }
    }
}