package org.petuum.ps.client.thread;

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.petuum.ps.row.RowUpdate;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class RowOpLogSerializer {

    private int myCommChannelIdx;
    private TIntObjectMap<ArrayList<SerializedOpLogBuffer>> bufferMap;

    public RowOpLogSerializer(int myCommChannelIdx) {
        this.myCommChannelIdx = myCommChannelIdx;
        this.bufferMap = new TIntObjectHashMap<>();
    }

    public int appendRowOpLog(int rowId, RowUpdate rowOplog) {

        int serverId = GlobalContext.getPartitionServerId(rowId,
                myCommChannelIdx);

        ArrayList<SerializedOpLogBuffer> bufferList = null;
        if (bufferMap.containsKey(serverId)) {
            bufferList = bufferMap.get(serverId);
        }
        if (bufferList == null) {
            bufferList = new ArrayList<>();
            bufferMap.put(serverId, bufferList);
            bufferList.add(new SerializedOpLogBuffer());
        }

        SerializedOpLogBuffer buffer = bufferList.get(bufferList.size() - 1);
        int serializedSize = buffer.appendRowOpLog(rowId, rowOplog);
        if (serializedSize == 0) {
            SerializedOpLogBuffer newBuffer = new SerializedOpLogBuffer();
            serializedSize = newBuffer.appendRowOpLog(rowId, rowOplog);
            assert (serializedSize > 0);
            bufferList.add(newBuffer);
        }
        return serializedSize;
    }

    // assume map entries are already reset to 0
    public void getServerTableSizeMap(
            TIntIntMap tableNumBytesByServer) {
        for (TIntObjectIterator<ArrayList<SerializedOpLogBuffer>> buffPair = bufferMap
                .iterator(); buffPair.hasNext(); ) {
            buffPair.advance();
            ArrayList<SerializedOpLogBuffer> buffList = buffPair.value();
            assert (buffList.size() > 0);
            int perServerTableSize = tableNumBytesByServer.get(buffPair
                    .key());
            perServerTableSize = 0;
            for (SerializedOpLogBuffer buff : buffList) {
                perServerTableSize += buff.getSize();
            }
        }
    }

    public void serializeByServer(TIntObjectMap<ByteBuffer> bytesByServer) {
        for (TIntObjectIterator<ByteBuffer> serverBytes = bytesByServer
                .iterator(); serverBytes.hasNext(); ) {
            serverBytes.advance();
            int serverId = serverBytes.key();
            ByteBuffer mem = serverBytes.value();
            int position = mem.position();
            int numRows = 0;

            ArrayList<SerializedOpLogBuffer> buffList = null;
            if (bufferMap.containsKey(serverId)) {
                buffList = bufferMap.get(serverId);
            }
            assert (buffList != null);

            int memOffset = Integer.SIZE / Byte.SIZE;

            for (SerializedOpLogBuffer buff : buffList) {

                buff.getMem().get(mem.array(), memOffset, buff.getSize());
                numRows += buff.getNumRowOplogs();
                memOffset += buff.getSize();
            }
            mem.putInt(position, numRows);
            bufferMap.remove(serverId);
        }
    }

}
