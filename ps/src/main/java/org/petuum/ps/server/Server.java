package org.petuum.ps.server;

import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntObjectProcedure;
import org.petuum.ps.client.thread.GlobalContext;
import org.petuum.ps.common.util.IntBox;
import org.petuum.ps.common.util.PtrBox;
import org.petuum.ps.common.util.VectorClock;
import org.petuum.ps.config.Config;
import org.petuum.ps.config.TableInfo;
import org.petuum.ps.row.RowUpdate;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;

public class Server {
    // Assume a single row does not exceed this size!
    private final static int K_PUSH_ROW_MSG_SIZE_INIT = 4 * 1024 * 1024;
    private VectorClock bgClock;
    private TIntObjectHashMap<ServerTable> tables;
    // mapping <clock, table id> to an array of read requests
    private Map<Integer, Map<Integer, Vector<ServerRowRequest>>> clockBgRowRequests;
    // latest oplog version that I have received from a bg thread
    private Map<Integer, Integer> bgVersionMap;
    private int pushRowMsgDataSize;
    private int serverId;
    private int accumOplogCount;

    public Server() {
        this.bgClock = new VectorClock();
        this.bgVersionMap = new HashMap<>();
        this.tables = new TIntObjectHashMap<>();
        this.clockBgRowRequests = new HashMap<>();
    }

    public void shutdown() {

    }

    public void init(int serverId, int[] bgIds) {
        for (int bgId : bgIds) {
            bgClock.addClock(bgId, 0);
            bgVersionMap.put(bgId, -1);
        }

        pushRowMsgDataSize = K_PUSH_ROW_MSG_SIZE_INIT;

        this.serverId = serverId;

        accumOplogCount = 0;

    }

    public void createTable(int tableId, TableInfo tableInfo, Config rowConfig, Config rowUpdateConfig) {
        tables.put(tableId, new ServerTable(tableId, tableInfo, rowConfig, rowUpdateConfig));

        if (GlobalContext.getResumeClock() > 0) {
            ServerTable table = tables.get(tableId);
            table.readSnapShot(GlobalContext.getResumeDir(), serverId, tableId,
                    GlobalContext.getResumeClock());
        }
    }

    public ServerRow findCreateRow(int tableId, int rowId) {

        ServerTable serverTable = tables.get(tableId);
        ServerRow serverRow = serverTable.findRow(rowId);
        if (serverRow != null)
            return serverRow;

        serverRow = serverTable.createRow(rowId);

        return serverRow;

    }

    public boolean clockUntil(int bgId, int clock) {
        final int newClock = bgClock.tickUntil(bgId, clock);
        if (newClock != 0) {
            if (GlobalContext.getSnapShotClock() <= 0
                    || newClock % GlobalContext.getSnapShotClock() != 0)
                return true;
            tables.forEachEntry(new TIntObjectProcedure<ServerTable>() {

                @Override
                public boolean execute(int key, ServerTable value) {
                    value.TakeSnapShot(GlobalContext.getSnapShotDir(),
                            serverId, key, newClock);
                    return true;
                }
            });

            return true;
        }

        return false;
    }

    public void addRowRequest(int bgId, int tableId, int rowId, int clock) {
        ServerRowRequest serverRowRequest = new ServerRowRequest();
        serverRowRequest.bgId = bgId;
        serverRowRequest.tableId = tableId;
        serverRowRequest.rowId = rowId;
        serverRowRequest.clock = clock;

        if (clockBgRowRequests.get(clock) == null) {
            Map<Integer, Vector<ServerRowRequest>> newEntry = new HashMap<>();
            clockBgRowRequests.put(clock, newEntry);
        }

        if (clockBgRowRequests.get(clock).get(bgId) == null) {
            Vector<ServerRowRequest> newEntry = new Vector<>();
            clockBgRowRequests.get(clock).put(bgId, newEntry);
        }
        clockBgRowRequests.get(clock).get(bgId).addElement(serverRowRequest);
    }

    public void getFulfilledRowRequests(ArrayList<ServerRowRequest> requests) {
        int clock = bgClock.getMinClock();
        requests.clear();

        Map<Integer, Vector<ServerRowRequest>> bgRowRequests = clockBgRowRequests
                .get(clock);
        if (bgRowRequests == null) {
            return;
        }
        for (Entry<Integer, Vector<ServerRowRequest>> bgRowRequestEntry : bgRowRequests
                .entrySet()) {
            requests.addAll(bgRowRequestEntry.getValue());
        }

        clockBgRowRequests.remove(clock);

    }

    public void applyOpLogUpdateVersion(ByteBuffer oplog, int oplogSize,
                                        int bgThreadId, int version) {
        bgVersionMap.put(bgThreadId, version);
        if (oplogSize == 0)
            return;

        SerializedOpLogReader serializedOpLogReader = new SerializedOpLogReader(
                oplog, tables);
        boolean toRead = serializedOpLogReader.restart();

        if (!toRead)
            return;

        IntBox tableId = new IntBox();
        IntBox rowId = new IntBox();
        ArrayList<Integer> columnIds = new ArrayList<>(); // the variable
        // pointer points to
        // memory
        IntBox numUpdates = new IntBox();
        PtrBox<Boolean> startedNewTable = new PtrBox<>();

        RowUpdate updates = serializedOpLogReader.next(tableId, rowId,
                columnIds, numUpdates, startedNewTable);

        ServerTable serverTable = tables.get(tableId.intValue);

        while (true) {
            ++accumOplogCount;
            boolean found = serverTable.applyRowOpLog(rowId.intValue, updates);

            if (!found) {
                serverTable.createRow(rowId.intValue);
                serverTable.applyRowOpLog(rowId.intValue, updates);
            }

            updates = serializedOpLogReader.next(tableId, rowId,
                    columnIds, numUpdates, startedNewTable);
            if (updates == null) {
                break;
            }

            if (startedNewTable.value) {
                serverTable = tables.get(tableId.intValue);
            }
        }
    }

    int getMinClock() {
        return bgClock.getMinClock();
    }

    int getBgVersion(int bgThreadId) {
        return bgVersionMap.get(bgThreadId);
    }

    /*public int createSendServerPushRowMsgs(pushMsgSendFunc PushMsgSend,
                                           boolean clockChanged) {

        TIntObjectMap<RecordBuff> buffs = new TIntObjectHashMap<>();
        HashMap<Integer, ServerPushRowMsg> msgMap = new HashMap<>();

        accumOplogCount = 0;

        int accumSendBytes = 0;

        int commChannelIdx = GlobalContext.getCommChannelIndexServer(serverId);

        // Create a message for each bg thread
        int clientId = 0;
        for (clientId = 0; clientId < GlobalContext.getNumClients(); ++clientId) {
            ServerPushRowMsg msg = new ServerPushRowMsg(pushRowMsgDataSize);
            msgMap.put(clientId, msg);
            buffs.put(clientId, new RecordBuff(msg.getData()));
        }

        int numTablesLeft = GlobalContext.getNumTables();

        TIntObjectIterator<ServerTable> iter = tables.iterator();
        while (iter.hasNext()) {
            iter.advance();
            int tableId = iter.key();
            ServerTable serverTable = iter.value();
            for (clientId = 0; clientId < GlobalContext.getNumClients(); ++clientId) {
                RecordBuff recordBuff = buffs.get(clientId);
                ByteBuffer tableIdPtr = recordBuff.getMemPtrInt32();
                if (tableIdPtr == null) {
                    int bgId = GlobalContext.getBgThreadId(clientId,
                            commChannelIdx);
                    PushMsgSend.invoke(bgId, msgMap.get(clientId),
                            false && clockChanged, getBgVersion(bgId),
                            getMinClock());

                    ServerPushRowMsg msg = new ServerPushRowMsg(
                            pushRowMsgDataSize);
                    msgMap.put(clientId, msg);
                    recordBuff.resetMem(msg.getData());
                    tableIdPtr = recordBuff.getMemPtrInt32();
                }
                tableIdPtr.putInt(tableId);
            }

            // ServerTable packs the data.
            serverTable.initAppendTableToBuffs();
            IntBox failedClientId = new IntBox(0);
            boolean packSuc = serverTable.appendTableToBuffs(0, buffs,
                    failedClientId, false);

            while (!packSuc) {
                RecordBuff recordBuff = buffs.get(failedClientId.intValue);
                ByteBuffer buffEndPtr = recordBuff.getMemPtrInt32();
                if (buffEndPtr != null)
                    buffEndPtr.putInt(GlobalContext.getSerializedTableEnd());

                int bgId = GlobalContext.getBgThreadId(failedClientId.intValue,
                        commChannelIdx);
                PushMsgSend.invoke(bgId, msgMap.get(failedClientId.intValue),
                        false && clockChanged, getBgVersion(bgId),
                        getMinClock());
                ServerPushRowMsg msg = new ServerPushRowMsg(pushRowMsgDataSize);
                msgMap.put(failedClientId.intValue, msg);
                recordBuff.resetMem(msg.getData());

                ByteBuffer tableIdPtr = recordBuff.getMemPtrInt32();
                tableIdPtr.putInt(tableId);
                packSuc = serverTable.appendTableToBuffs(
                        failedClientId.intValue, buffs, failedClientId, true);
            }
            --numTablesLeft;
            if (numTablesLeft > 0) {
                for (clientId = 0; clientId < GlobalContext.getNumClients(); ++clientId) {

                    RecordBuff recordBuff = buffs.get(clientId);
                    ByteBuffer tableSepPtr = recordBuff.getMemPtrInt32();
                    if (tableSepPtr == null) {
                        int bgId = GlobalContext.getBgThreadId(clientId,
                                commChannelIdx);

                        PushMsgSend.invoke(bgId, msgMap.get(clientId),
                                false && clockChanged, getBgVersion(bgId),
                                getMinClock());
                        ServerPushRowMsg msg = new ServerPushRowMsg(
                                pushRowMsgDataSize);
                        msgMap.put(clientId, msg);
                        recordBuff.resetMem(msg.getData());

                    } else {
                        tableSepPtr.putInt(GlobalContext
                                .getSerializedTableSeparator());
                    }
                }
            } else {
                break;
            }
        }

        for (clientId = 0; clientId < GlobalContext.getNumClients(); ++clientId) {
            RecordBuff recordBuff = buffs.get(clientId);
            ByteBuffer tableEndPtr = recordBuff.getMemPtrInt32();
            if (tableEndPtr == null) {
                int bg_id = GlobalContext.getBgThreadId(clientId,
                        commChannelIdx);
                PushMsgSend.invoke(bg_id, msgMap.get(clientId),
                        true && clockChanged, getBgVersion(bg_id),
                        getMinClock());
                continue;
            }
            tableEndPtr.putInt(GlobalContext.getSerializedTableEnd());

            msgMap.get(clientId).putAvaiSize(
                    buffs.get(clientId).getMemUsedSize());
            accumSendBytes += msgMap.get(clientId).get_size();

            int bgThreadId = GlobalContext.getBgThreadId(clientId,
                    commChannelIdx);
            PushMsgSend.invoke(bgThreadId, msgMap.get(clientId),
                    true && clockChanged, getBgVersion(bgThreadId),
                    getMinClock());
        }
        return accumSendBytes;
    }

    public int createSendServerPushRowMsgs(pushMsgSendFunc pushMsgSender) {
        return createSendServerPushRowMsgs(pushMsgSender, true);
    }

    int createSendServerPushRowMsgsPartial(pushMsgSendFunc pushMsgSendFunc) {
        return 0;
    }

    boolean accumedOpLogSinceLastPush() {
        return accumOplogCount > 0;
    }

    public static abstract class pushMsgSendFunc {
        public abstract void invoke(int bgId, ServerPushRowMsg msg,
                                    boolean isLast, int version, int serverMinClock);
    }*/

}
