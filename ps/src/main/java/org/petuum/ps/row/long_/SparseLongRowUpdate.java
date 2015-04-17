package org.petuum.ps.row.long_;

import gnu.trove.iterator.TIntLongIterator;
import gnu.trove.map.TIntLongMap;
import gnu.trove.map.hash.TIntLongHashMap;
import org.petuum.ps.row.RowUpdate;

import java.nio.ByteBuffer;

/**
 * Created by aqiao on 3/3/15.
 */
public class SparseLongRowUpdate implements LongRowUpdate {

    private TIntLongMap updates;

    public SparseLongRowUpdate() {
        this.updates = new TIntLongHashMap();
    }

    private SparseLongRowUpdate(SparseLongRowUpdate other) {
        this.updates = new TIntLongHashMap();
        this.updates.putAll(other.updates);
    }

    SparseLongRowUpdate(ByteBuffer data) {
        this.updates = new TIntLongHashMap();
        int size = data.getInt();
        for (int i = 0; i < size; i++) {
            this.updates.put(data.getInt(), data.getLong());
        }
    }

    @Override
    public long getUpdate(int columnId) {
        return this.updates.get(columnId);
    }

    @Override
    public void setUpdate(int columnId, long update) {
        this.updates.put(columnId, update);
    }

    @Override
    public void incUpdate(int columnId, long update) {
        this.updates.adjustOrPutValue(columnId, update, update);
    }

    @Override
    public LongUpdateIterator iterator() {
        return new SparseLongUpdateIterator();
    }

    @Override
    public RowUpdate getCopy() {
        return new SparseLongRowUpdate(this);
    }

    @Override
    public void addRowUpdate(RowUpdate rowUpdate) {
        LongUpdateIterator it = ((LongRowUpdate) rowUpdate).iterator();
        while (it.hasNext()) {
            it.advance();
            this.updates.adjustOrPutValue(it.getColumnId(), it.getUpdate(), it.getUpdate());
        }
    }

    @Override
    public int getSerializedSize() {
        return (Integer.SIZE + this.updates.size() * (Integer.SIZE + Long.SIZE)) / Byte.SIZE;
    }

    @Override
    public ByteBuffer serialize() {
        ByteBuffer ret = ByteBuffer.allocate(this.getSerializedSize());
        ret.putInt(this.updates.size());
        TIntLongIterator it = this.updates.iterator();
        while (it.hasNext()) {
            it.advance();
            ret.putInt(it.key());
            ret.putLong(it.value());
        }
        return ret;
    }

    @Override
    public void clear() {
        this.updates.clear();
    }

    private class SparseLongUpdateIterator implements LongUpdateIterator {

        TIntLongIterator it;

        public SparseLongUpdateIterator() {
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
        public long getUpdate() {
            return this.it.value();
        }
    }
}
