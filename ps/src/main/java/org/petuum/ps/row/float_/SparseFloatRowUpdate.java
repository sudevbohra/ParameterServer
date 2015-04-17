package org.petuum.ps.row.float_;

import gnu.trove.iterator.TIntFloatIterator;
import gnu.trove.map.TIntFloatMap;
import gnu.trove.map.hash.TIntFloatHashMap;
import org.petuum.ps.row.RowUpdate;

import java.nio.ByteBuffer;

/**
 * Created by aqiao on 3/3/15.
 */
public class SparseFloatRowUpdate implements FloatRowUpdate {

    private TIntFloatMap updates;

    public SparseFloatRowUpdate() {
        this.updates = new TIntFloatHashMap();
    }

    private SparseFloatRowUpdate(SparseFloatRowUpdate other) {
        this.updates = new TIntFloatHashMap();
        this.updates.putAll(other.updates);
    }

    SparseFloatRowUpdate(ByteBuffer data) {
        this.updates = new TIntFloatHashMap();
        int size = data.getInt();
        for (int i = 0; i < size; i++) {
            this.updates.put(data.getInt(), data.getFloat());
        }
    }

    @Override
    public float getUpdate(int columnId) {
        return this.updates.get(columnId);
    }

    @Override
    public void setUpdate(int columnId, float update) {
        this.updates.put(columnId, update);
    }

    @Override
    public void incUpdate(int columnId, float update) {
        this.updates.adjustOrPutValue(columnId, update, update);
    }

    @Override
    public FloatUpdateIterator iterator() {
        return new SparseFloatUpdateIterator();
    }

    @Override
    public RowUpdate getCopy() {
        return new SparseFloatRowUpdate(this);
    }

    @Override
    public void addRowUpdate(RowUpdate rowUpdate) {
        FloatUpdateIterator it = ((FloatRowUpdate) rowUpdate).iterator();
        while (it.hasNext()) {
            it.advance();
            this.updates.adjustOrPutValue(it.getColumnId(), it.getUpdate(), it.getUpdate());
        }
    }

    @Override
    public int getSerializedSize() {
        return (Integer.SIZE + this.updates.size() * (Integer.SIZE + Float.SIZE)) / Byte.SIZE;
    }

    @Override
    public ByteBuffer serialize() {
        ByteBuffer ret = ByteBuffer.allocate(this.getSerializedSize());
        ret.putInt(this.updates.size());
        TIntFloatIterator it = this.updates.iterator();
        while (it.hasNext()) {
            it.advance();
            ret.putInt(it.key());
            ret.putFloat(it.value());
        }
        return ret;
    }

    @Override
    public void clear() {
        this.updates.clear();
    }

    private class SparseFloatUpdateIterator implements FloatUpdateIterator {

        TIntFloatIterator it;

        public SparseFloatUpdateIterator() {
            this.it = updates.iterator();
        }

        @Override
        public boolean hasNext() {
            return this.it.hasNext();
        }

        @Override
        public void advance() {
            this.it.advance();
        }

        @Override
        public int getColumnId() {
            return this.it.key();
        }

        @Override
        public float getUpdate() {
            return this.it.value();
        }
    }
}
