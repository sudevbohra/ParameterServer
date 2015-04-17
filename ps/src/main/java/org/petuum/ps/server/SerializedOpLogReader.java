package org.petuum.ps.server;

import gnu.trove.map.hash.TIntObjectHashMap;
import org.petuum.ps.common.util.IntBox;
import org.petuum.ps.common.util.PtrBox;
import org.petuum.ps.row.RowUpdate;
import org.petuum.ps.row.RowUpdateFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class SerializedOpLogReader {

    private ByteBuffer serializedOplogPtr;
    private int numTablesLeft;
    private int currentTableId;
    private int numRowsLeftInCurrentTable;
    private RowUpdateFactory currentRowUpdateFactory;

    private TIntObjectHashMap<ServerTable> serverTables;

    public SerializedOpLogReader(ByteBuffer oplogPtr,
                                 TIntObjectHashMap<ServerTable> serverTables) {
        this.serializedOplogPtr = oplogPtr;
        this.serverTables = serverTables;

    }

    public boolean restart() {
        serializedOplogPtr.rewind();
        numTablesLeft = serializedOplogPtr.getInt();
        if (numTablesLeft == 0)
            return false;
        startNewTable();
        return true;
    }

    public RowUpdate next(IntBox tableId, IntBox rowId,
                          ArrayList<Integer> columnIds, IntBox numUpdates,
                          PtrBox<Boolean> startedNewTable) {

        if (numTablesLeft == 0)
            return null;
        startedNewTable.value = false;
        while (true) {

            if (numRowsLeftInCurrentTable > 0) {
                tableId.intValue = currentTableId;
                rowId.intValue = serializedOplogPtr.getInt();

                IntBox serialized_size = new IntBox(0);
                RowUpdate rowUpdate = currentRowUpdateFactory.deserialize(serializedOplogPtr);
                // offset
                numRowsLeftInCurrentTable--;
                return rowUpdate;
            } else {
                --numTablesLeft;
                if (numTablesLeft > 0) {
                    startNewTable();
                    startedNewTable.value = true;
                    continue;
                } else {
                    return null;
                }
            }
        }

    }

    private void startNewTable() {
        currentTableId = serializedOplogPtr.getInt();
        int updateSize = serializedOplogPtr.getInt();
        numRowsLeftInCurrentTable = serializedOplogPtr.getInt();
        ServerTable serverTable = serverTables.get(currentTableId);
        currentRowUpdateFactory = serverTable.getRowUpdateFactory();
    }

}
