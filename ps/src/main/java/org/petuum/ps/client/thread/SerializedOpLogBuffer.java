package org.petuum.ps.client.thread;

import org.petuum.ps.row.RowUpdate;

import java.nio.ByteBuffer;

public class SerializedOpLogBuffer {

    private static final int CAPACITY = 1 * 1024 * 1024;
    private int size;
    private ByteBuffer mem;
    private int numRowOplogs;

    public SerializedOpLogBuffer() {
        this.size = 0;
        this.mem = ByteBuffer.allocate(CAPACITY);
        this.numRowOplogs = 0;
    }

    public int appendRowOpLog(int rowId, RowUpdate rowOplog) {
        int serializedSize = rowOplog.getSerializedSize();
        if (size + Integer.SIZE / Byte.SIZE + serializedSize > CAPACITY)
            return 0;
        mem.putInt(size, rowId);
        size += Integer.SIZE / Byte.SIZE;

        mem.position(size);
        ByteBuffer serialized = rowOplog.serialize();
        serialized.rewind();
        serializedSize = serialized.capacity();
        mem.put(serialized);
        size += serializedSize;
        ++numRowOplogs;

        return Integer.SIZE / Byte.SIZE + serializedSize;
    }

    public int getSize() {
        return size;
    }

    public ByteBuffer getMem() {
        return mem;
    }

    public int getNumRowOplogs() {
        return numRowOplogs;
    }
}
