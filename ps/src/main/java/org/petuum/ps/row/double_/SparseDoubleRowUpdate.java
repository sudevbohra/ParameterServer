package org.petuum.ps.row.double_;

import gnu.trove.iterator.TIntDoubleIterator;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import org.petuum.ps.row.RowUpdate;

import java.nio.ByteBuffer;

/**
 * Created by aqiao on 3/3/15.
 */
public class SparseDoubleRowUpdate implements DoubleRowUpdate {

    private TIntDoubleMap updates;

    public SparseDoubleRowUpdate() {
        this.updates = new TIntDoubleHashMap();
    }

    private SparseDoubleRowUpdate(SparseDoubleRowUpdate other) {
        this.updates = new TIntDoubleHashMap();
        this.updates.putAll(other.updates);
    }

    SparseDoubleRowUpdate(ByteBuffer data) {
        this.updates = new TIntDoubleHashMap();
        int size = data.getInt();
        for (int i = 0; i < size; i++) {
            this.updates.put(data.getInt(), data.getDouble());
        }
    }

    @Override
    public double getUpdate(int columnId) {
        return this.updates.get(columnId);
    }

    @Override
    public void setUpdate(int columnId, double update) {
        this.updates.put(columnId, update);
    }

    @Override
    public void incUpdate(int columnId, double update) {
        this.updates.adjustOrPutValue(columnId, update, update);
    }

    @Override
    public DoubleUpdateIterator iterator() {
        return new SparseDoubleUpdateIterator();
    }

    @Override
    public RowUpdate getCopy() {
        return new SparseDoubleRowUpdate(this);
    }

    @Override
    public void addRowUpdate(RowUpdate rowUpdate) {
        DoubleUpdateIterator it = ((DoubleRowUpdate) rowUpdate).iterator();
        while (it.hasNext()) {
            it.advance();
            this.updates.adjustOrPutValue(it.getColumnId(), it.getUpdate(), it.getUpdate());
        }
    }

    @Override
    public int getSerializedSize() {
        return (Integer.SIZE + this.updates.size() * (Integer.SIZE + Double.SIZE)) / Byte.SIZE;
    }

    @Override
    public ByteBuffer serialize() {
        ByteBuffer ret = ByteBuffer.allocate(this.getSerializedSize());
        ret.putInt(this.updates.size());
        TIntDoubleIterator it = this.updates.iterator();
        while (it.hasNext()) {
            it.advance();
            ret.putInt(it.key());
            ret.putDouble(it.value());
        }
        return ret;
    }

    @Override
    public void clear() {
        this.updates.clear();
    }

    private class SparseDoubleUpdateIterator implements DoubleUpdateIterator {

        TIntDoubleIterator it;

        public SparseDoubleUpdateIterator() {
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
        public double getUpdate() {
            return this.it.value();
        }
    }
}
