package org.petuum.ps.row.int_;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import org.petuum.ps.row.RowUpdate;

import java.nio.ByteBuffer;

/**
 * Created by aqiao on 3/3/15.
 */
public class SparseIntRowUpdate implements IntRowUpdate {

    private TIntIntMap updates;

    public SparseIntRowUpdate() {
        this.updates = new TIntIntHashMap();
    }

    private SparseIntRowUpdate(SparseIntRowUpdate other) {
        this.updates = new TIntIntHashMap();
        this.updates.putAll(other.updates);
    }

    SparseIntRowUpdate(ByteBuffer data) {
        this.updates = new TIntIntHashMap();
        int size = data.getInt();
        for (int i = 0; i < size; i++) {
            this.updates.put(data.getInt(), data.getInt());
        }
    }

    @Override
    public int getUpdate(int columnId) {
        return this.updates.get(columnId);
    }

    @Override
    public void setUpdate(int columnId, int update) {
        this.updates.put(columnId, update);
    }

    @Override
    public void incUpdate(int columnId, int update) {
        this.updates.adjustOrPutValue(columnId, update, update);
    }

    @Override
    public IntUpdateIterator iterator() {
        return new SparseIntUpdateIterator();
    }

    @Override
    public RowUpdate getCopy() {
        return new SparseIntRowUpdate(this);
    }

    @Override
    public void addRowUpdate(RowUpdate rowUpdate) {
        IntUpdateIterator it = ((IntRowUpdate) rowUpdate).iterator();
        while (it.hasNext()) {
            it.advance();
            this.updates.adjustOrPutValue(it.getColumnId(), it.getUpdate(), it.getUpdate());
        }
    }

    @Override
    public int getSerializedSize() {
        return (Integer.SIZE + this.updates.size() * (Integer.SIZE + Integer.SIZE)) / Byte.SIZE;
    }

    @Override
    public ByteBuffer serialize() {
        ByteBuffer ret = ByteBuffer.allocate(this.getSerializedSize());
        ret.putInt(this.updates.size());
        TIntIntIterator it = this.updates.iterator();
        while (it.hasNext()) {
            it.advance();
            ret.putInt(it.key());
            ret.putInt(it.value());
        }
        return ret;
    }

    @Override
    public void clear() {
        this.updates.clear();
    }

    private class SparseIntUpdateIterator implements IntUpdateIterator {

        TIntIntIterator it;

        public SparseIntUpdateIterator() {
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
        public int getUpdate() {
            return this.it.value();
        }
    }
}
