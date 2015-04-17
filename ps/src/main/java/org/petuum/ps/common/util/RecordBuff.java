package org.petuum.ps.common.util;

import java.nio.ByteBuffer;

public class RecordBuff {

    private ByteBuffer memBuff;

    public RecordBuff(ByteBuffer mem) {
        this.memBuff = mem;
        memBuff.rewind();
    }

    public RecordBuff(RecordBuff other) {
        this.memBuff = other.memBuff;
    }

    // does not take ownership of the memory
    public ByteBuffer resetMem(ByteBuffer mem) {
        ByteBuffer old_mem = memBuff;
        memBuff = mem;
        memBuff.rewind();
        return old_mem;
    }

    public void ResetOffset() {
        memBuff.rewind();
    }

    public boolean Append(int record_id, ByteBuffer record, int record_size) {
        if (memBuff.capacity() + 4 + record_size + 4 > memBuff.limit()) {
            return false;
        }
        memBuff.putInt(record_id);
        memBuff.putInt(record_size);
        memBuff.put(record);
        return true;

    }

    public int getMemUsedSize() {
        return memBuff.capacity();
    }

    // TODO: This should not be used. If this creates a problem, fix it.
    public ByteBuffer getMemPtrInt32() {
        if (memBuff.capacity() + 4 > memBuff.limit()) {
            return null;
        }
        return memBuff;
    }

    public void PrintInfo() {
        System.out.println(memBuff.toString());
    }
}
