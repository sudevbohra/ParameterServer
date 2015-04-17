package org.petuum.ps.client.thread;

import com.google.common.base.Preconditions;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.petuum.ps.client.ClientRow;
import org.petuum.ps.client.ClientTable;
import org.petuum.ps.client.OpLogSerializer;
import org.petuum.ps.client.oplog.Oplog;
import org.petuum.ps.client.storage.ProcessStorage;
import org.petuum.ps.common.network.CommBus;
import org.petuum.ps.common.network.CommBus.WaitMsgTimeOutFunc;
import org.petuum.ps.common.msg.*;
import org.petuum.ps.common.network.Msg;
import org.petuum.ps.common.util.IntBox;
import org.petuum.ps.config.Config;
import org.petuum.ps.config.ConsistencyModel;
import org.petuum.ps.config.HostInfo;
import org.petuum.ps.config.TableConfig;
import org.petuum.ps.row.Row;
import org.petuum.ps.row.RowUpdate;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public abstract class AbstractBgWorker extends Thread {

    protected CommBus commBus;
    protected int myId;
    protected WaitMsgTimeOutFunc waitMsg;
    protected ArrayList<Integer> serverIds;
    protected CyclicBarrier initBarrier;
    protected CyclicBarrier createTableBarrier;
    protected int myCommChannelIdx;
    protected TIntObjectMap<ClientTable> tables;
    protected int version;
    protected RowRequestOpLogMgr rowRequestOplogMgr;
    protected int clientClock;
    protected int clockHasPushed;
    // initialized at Creation time, used in CreateSendOpLogs()
    // For server x, table y, the size of serialized OpLog is ...
    protected TIntObjectMap<TIntIntMap> serverTableOplogSizeMap;
    // The OpLog msg to each server
    protected TIntObjectMap<ClientSendOpLogMsg> serverOplogMsgMap;
    protected TIntObjectMap<RowOpLogSerializer> rowOplogSerializerMap;
    // protected TIntObjectMap<IntBox> tableNumBytesByServer;
    protected TIntIntMap tableNumBytesByServer;

    public AbstractBgWorker(int id, int commChannelIdx,
            TIntObjectMap<ClientTable> tables, CyclicBarrier initBarrier,
            CyclicBarrier createTableBarrier) {
        this.myId = id;
        this.myCommChannelIdx = commChannelIdx;
        this.tables = tables;
        this.version = 0;
        this.clientClock = 0;
        this.clockHasPushed = -1;
        this.commBus = GlobalContext.commBus;
        this.initBarrier = initBarrier;
        this.createTableBarrier = createTableBarrier;
        this.serverTableOplogSizeMap = new TIntObjectHashMap<>();
        this.serverOplogMsgMap = new TIntObjectHashMap<>();
        this.rowOplogSerializerMap = new TIntObjectHashMap<>();
        this.tableNumBytesByServer = new TIntIntHashMap();
        this.serverIds = GlobalContext.getServerThreadIDs(myCommChannelIdx);
        for (Integer serverId : serverIds) {
            this.serverTableOplogSizeMap.put(serverId, new TIntIntHashMap());
            this.serverOplogMsgMap.put(serverId, null);
            this.tableNumBytesByServer.put(serverId, 0);
        }
    }

    public boolean sendMsg(Msg msg) {
        return commBus.sendInproc(myId, msg);
    }

    public void appThreadDeregister() {
        sendMsg(new Msg(MsgType.APP_THREAD_DEREG));
    }

    public void appThreadRegister() {
        commBus.connectTo(myId, new Msg(MsgType.APP_CONNECT));
    }

    public void clockAllTables() {
        sendMsg(new Msg(MsgType.BG_CLOCK));
    }

    public boolean createTable(int tableId, TableConfig tableConfig) {
        Config rowConfig = tableConfig.getRowConfig();
        Config rowUpdateConfig = tableConfig.getRowUpdateConfig();
        BgCreateTableMsg bgCreateTableMsg = new BgCreateTableMsg();
        bgCreateTableMsg.setTableId(tableId);
        bgCreateTableMsg.setStaleness(tableConfig.getStaleness());
        bgCreateTableMsg.setRowType(tableConfig.getRowType());
        bgCreateTableMsg.setProcessCacheCapacity(tableConfig
                .getProcessCacheCapacity());
        bgCreateTableMsg.setThreadCacheCapacity(tableConfig
                .getThreadCacheCapacity());
        bgCreateTableMsg.setRowOplogType(tableConfig.getRowUpdateType());
        bgCreateTableMsg.setNoOplogReplay(tableConfig.isNoOplogReplay());
        bgCreateTableMsg.setRowConfig(rowConfig);
        bgCreateTableMsg.setRowUpdateConfig(rowUpdateConfig);

        Preconditions.checkArgument(sendMsg(bgCreateTableMsg));
        // waiting for response

        Msg msg = commBus.recv();
        assert(msg.getMsgType() == MsgType.CREATE_TABLE_REPLY);

        return true;
    }

    public boolean requestRow(int tableId, int rowId, int clock) {
        RowRequestMsg requestRowMsg = new RowRequestMsg();
        requestRowMsg.setTableId(tableId);
        requestRowMsg.setRowId(rowId);
        requestRowMsg.setClock(clock);
        requestRowMsg.setForcedRequest(false);

        sendMsg(requestRowMsg);

        // Wait for response
        Msg msg = commBus.recv();
        assert(msg.getMsgType() == MsgType.ROW_REQUEST_REPLY);

        return true;
    }

    public void requestRowAsync(int tableId, int rowId, int clock,
            boolean forced) {
        RowRequestMsg requestRowMsg = new RowRequestMsg();
        requestRowMsg.setTableId(tableId);
        requestRowMsg.setRowId(rowId);
        requestRowMsg.setClock(clock);
        requestRowMsg.setForcedRequest(forced);

        sendMsg(requestRowMsg);
    }

    public void sendOpLogsAllTables() {
        sendMsg(new Msg(MsgType.BG_SEND_OP_LOG));
    }

    protected void initWhenStart() {
        this.setWaitMsg();
        this.createRowRequestOpLogMgr();
    }

    protected void setWaitMsg() {
        if (GlobalContext.getAggressiveCpu()) {
            this.waitMsg = new WaitMsgBusy();
        } else {
            this.waitMsg = new WaitMsgSleep();
        }
    }

    protected abstract void createRowRequestOpLogMgr();

    protected void initCommBus() {
        CommBus.Config commConfig = new CommBus.Config();
        commConfig.entityId = this.myId;
        commConfig.lType = CommBus.K_IN_PROC;
        this.commBus.threadRegister(commConfig);
    }

    protected void bgServerHandshake() {
        // connect to name node
        int nameNodeId = GlobalContext.getNameNodeId();
        this.connectToNameNodeOrServer(nameNodeId);

        // wait for ConnectServerMsg
        Msg msg = this.commBus.recv();

        assert(msg.getSender() == nameNodeId);
        assert(msg.getMsgType() == MsgType.CONNECT_SERVER);

        // connect to servers
        for (int serverId : this.serverIds) {
            connectToNameNodeOrServer(serverId);
        }

        // get messages from servers for permission to start
        int numStartedServers = 0;
        for (numStartedServers = 0; numStartedServers < GlobalContext
                .getNumClients() + 1; ++numStartedServers) {
            msg = this.commBus.recv();
            Preconditions.checkArgument(msg.getMsgType() == MsgType.CLIENT_START);
        }
    }

    protected void connectToNameNodeOrServer(int serverId) {
        ClientConnectMsg clientConnectMsg = new ClientConnectMsg();
        clientConnectMsg.setClientId(GlobalContext.getClientId());

        if (this.commBus.isLocalEntity(serverId)) {
            this.commBus.connectTo(serverId, clientConnectMsg);
        } else {
            HostInfo serverInfo;
            if (serverId == GlobalContext.getNameNodeId()) {
                serverInfo = GlobalContext.getNameNodeInfo();
            } else {
                serverInfo = GlobalContext.getServerInfo(serverId);
            }
            String serverAddr = serverInfo.ip + ":" + serverInfo.port;
            this.commBus.connectTo(serverId, serverAddr, clientConnectMsg);
        }
    }

    protected void recvAppInitThreadConnection(IntBox numConnectedAppThreads) {
        Msg msg = commBus.recv();
        assert(msg.getMsgType() == MsgType.APP_CONNECT);
        numConnectedAppThreads.intValue++;
        assert(numConnectedAppThreads.intValue <= GlobalContext.getNumAppThreads());
    }

    protected void handleCreateTables() {
        for (int numCreatedTables = 0; numCreatedTables < GlobalContext
                .getNumTables(); ++numCreatedTables) {
            int tableId;
            TableConfig tableConfig = new TableConfig();

            Msg msg = this.commBus.recv();
            int senderId = msg.getSender();
            assert(msg.getMsgType() == MsgType.BG_CREATE_TABLE);
            BgCreateTableMsg bgCreateTableMsg = BgCreateTableMsg.wrap(msg);

            tableConfig
                    .setStaleness(bgCreateTableMsg.getStaleness())
                    .setRowType(bgCreateTableMsg.getRowType())
                    .setProcessCacheCapacity(
                            bgCreateTableMsg.getProcessCacheCapacity())
                    .setThreadCacheCapacity(
                            bgCreateTableMsg.getThreadCacheCapacity())
                    .setRowUpdateType(bgCreateTableMsg.getRowOplogType())
                    .setNoOplogReplay(bgCreateTableMsg.getNoOplogReplay());

            Config rowConfig = bgCreateTableMsg.getRowConfig();
            Config rowUpdateConfig = bgCreateTableMsg.getRowUpdateConfig();

            tableConfig.setRowConfig(rowConfig);
            tableConfig.setRowUpdateConfig(rowUpdateConfig);

            CreateTableMsg createTableMsg = new CreateTableMsg();
            createTableMsg.setTableId(bgCreateTableMsg.getTableId());
            createTableMsg.setStaleness(bgCreateTableMsg.getStaleness());
            createTableMsg.setRowType(bgCreateTableMsg.getRowType());
            createTableMsg.setRowCapacity(bgCreateTableMsg.getRowCapacity());
            createTableMsg.setOplogDenseSerialized(bgCreateTableMsg
                    .getOplogDenseSerialized());
            createTableMsg.setRowOplogType(bgCreateTableMsg.getRowOplogType());
            createTableMsg.setDenseRowOplogCapacity(bgCreateTableMsg
                    .getDenseRowOplogCapacity());
            createTableMsg.setRowConfig(rowConfig);
            createTableMsg.setRowUpdateConfig(rowUpdateConfig);

            tableId = createTableMsg.getTableId();

            // send msg to name node
            this.commBus.send(GlobalContext.getNameNodeId(), createTableMsg);

            // wait for response from name node
            msg = this.commBus.recv();

            assert(msg.getMsgType() == MsgType.CREATE_TABLE_REPLY);
            CreateTableReplyMsg createTableReplyMsg = CreateTableReplyMsg.wrap(msg);
            assert(createTableReplyMsg.getTableId() == tableId);

            ClientTable clientTable = new ClientTable(tableId, tableConfig);

            // not thread-safe
            this.tables.put(tableId, clientTable);

            this.commBus.sendInproc(senderId, createTableReplyMsg);
        }

        Msg msg = commBus.recv();
        assert(msg.getMsgType() == MsgType.CREATED_ALL_TABLES);
    }

    protected void finalizeTableStats() {
    }

    public void shutdown() {
        try {
            this.join();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    protected long bgIdleWork() {
        return 0;
    }

    protected long resetBgIdleMilli() {
        return 0;
    }

    protected void prepareBeforeInfiniteLoop() {
    }

    protected void checkForwardRowRequestToServer(int appThreadId,
            RowRequestMsg rowRequestMsg) {
        int tableId = rowRequestMsg.getTableId();
        int rowId = rowRequestMsg.getRowId();
        int clock = rowRequestMsg.getClock();
        boolean forced = rowRequestMsg.getForcedRequest();

        if (!forced) {
            // Check if the row exists in process cache
            ClientTable table = null;
            if (this.tables.containsKey(tableId)) {
                table = this.tables.get(tableId);
            }
            assert(table != null);
            // check if it is in process storage
            ProcessStorage tableStorage = table.getProcessStorage();
            ClientRow clientRow = tableStorage.getRow(rowId);
            if (clientRow != null) {
                if ((GlobalContext.getConsistencyModel() == ConsistencyModel.SSP && clientRow
                        .getClock() >= clock)
                        || (GlobalContext.getConsistencyModel() == ConsistencyModel.SSPPush)
                        || (GlobalContext.getConsistencyModel() == ConsistencyModel.SSPAggr)) {
                    this.commBus.sendInproc(appThreadId, new Msg(MsgType.ROW_REQUEST_REPLY));
                    return;
                }
            }
        }

        RowRequestInfo rowRequest = new RowRequestInfo();
        rowRequest.appThreadId = appThreadId;
        rowRequest.clock = rowRequestMsg.getClock();

        // Version in request denotes the update version that the row on server
        // can see. Which should be 1 less than the current version number.
        rowRequest.version = this.version - 1;

        boolean shouldBeSent = this.rowRequestOplogMgr.addRowRequest(
                rowRequest, tableId, rowId);

        if (shouldBeSent) {
            int serverId = GlobalContext.getPartitionServerId(rowId,
                    this.myCommChannelIdx);

            Preconditions.checkArgument(this.commBus.send(serverId,
                    rowRequestMsg));
        }
    }

    protected void handleServerRowRequestReply(int serverId,
            ServerRowRequestReplyMsg serverRowRequestReplyMsg) {
        int tableId = serverRowRequestReplyMsg.getTableId();
        int rowId = serverRowRequestReplyMsg.getRowId();
        int clock = serverRowRequestReplyMsg.getClock();
        int version = serverRowRequestReplyMsg.getVersion();

        // if (tableId == 1 && rowId == 2)
        // System.out.println("handleServerRowRequestReply tableId=" + tableId +
        // " rowId=" + rowId + " clock=" + clock + " version=" + version);

        ClientTable clientTable = this.tables.get(tableId);

        rowRequestOplogMgr.serverAcknowledgeVersion(serverId, version);

        ClientRow clientRow = clientTable.getProcessStorage().getRow(rowId);

        ByteBuffer data = serverRowRequestReplyMsg.getRowData();
        int rowSize = serverRowRequestReplyMsg.getRowSize();

        if (clientRow != null) {
            updateExistingRow(tableId, rowId, clientRow, clientTable, data,
                    rowSize, version);
            clientRow.setClock(clock);
        } else { // not found
            insertNonexistentRow(tableId, rowId, clientTable, data, rowSize,
                    version, clock);
        }

        ArrayList<Integer> appThreadIds = new ArrayList<>();
        int clockToRequest = rowRequestOplogMgr.informReply(tableId, rowId,
                clock, this.version, appThreadIds);

        if (clockToRequest >= 0) {
            RowRequestMsg rowRequestMsg = new RowRequestMsg();
            rowRequestMsg.setTableId(tableId);
            rowRequestMsg.setRowId(rowId);
            rowRequestMsg.setClock(clockToRequest);

            int partitionServerId = GlobalContext.getPartitionServerId(rowId,
                    myCommChannelIdx);

            Preconditions.checkArgument(commBus.send(partitionServerId,
                    rowRequestMsg));
        }

        Msg rowRequestReplyMsg = new Msg(MsgType.ROW_REQUEST_REPLY);

        for (Integer appThreadId : appThreadIds) {
            commBus.sendInproc(appThreadId, rowRequestReplyMsg);
        }
    }

    protected long handleClockMsg(boolean clockAdvanced) {
        BgOpLog bgOplog = prepareOpLogsToSend();

        createOpLogMsgs(bgOplog);

        this.clockHasPushed = this.clientClock;

        sendOpLogMsgs(clockAdvanced);
        trackBgOpLog(bgOplog);
        return 0;
    }

    protected abstract void trackBgOpLog(BgOpLog bgOplog);

    protected void createOpLogMsgs(BgOpLog bgOplog) {
        TIntObjectMap<TIntObjectMap<ByteBuffer>> tableServerMemMap = new TIntObjectHashMap<>();
        for (TIntObjectIterator<TIntIntMap> serverIter = serverTableOplogSizeMap
                .iterator(); serverIter.hasNext();) {
            serverIter.advance();

            OpLogSerializer oplogSerializer = new OpLogSerializer();
            int serverId = serverIter.key();
            int serverOplogMsgSize = oplogSerializer.init(serverIter.value());

            if (serverOplogMsgSize == 0) {
                serverOplogMsgMap.remove(serverId);
                continue;
            }

            serverOplogMsgMap.put(serverId, new ClientSendOpLogMsg(serverOplogMsgSize));

            oplogSerializer
                    .assignMem(serverOplogMsgMap.get(serverId).getData());

            for (TIntObjectIterator<ClientTable> tablePair = this.tables
                    .iterator(); tablePair.hasNext();) {
                tablePair.advance();
                int tableId = tablePair.key();
                if (!tableServerMemMap.containsKey(tableId)) {
                    tableServerMemMap.put(tableId,
                            new TIntObjectHashMap<ByteBuffer>());
                }
                ByteBuffer tablePtr = oplogSerializer.getTablePtr(tableId);

                if (tablePtr == null) {
                    tableServerMemMap.get(tableId).remove(serverId);
                    continue;
                }

                // table id
                tablePtr.putInt(tableId);

                // table update size
                tablePtr.putInt(0);

                // offset for table rows

                tableServerMemMap.get(tableId).put(serverId, tablePtr.slice());
            }
        }
        for (TIntObjectIterator<ClientTable> tablePair = this.tables.iterator(); tablePair
                .hasNext();) {
            tablePair.advance();
            int tableId = tablePair.key();
            if (!tableServerMemMap.containsKey(tableId)) {
                tableServerMemMap.put(tableId,
                        new TIntObjectHashMap<ByteBuffer>());
            }
            ClientTable table = tablePair.value();
            if (table.isNoOplogReplay()) {
                RowOpLogSerializer rowOplogSerializer = rowOplogSerializerMap
                        .get(tableId);

                rowOplogSerializer.serializeByServer(tableServerMemMap
                        .get(tableId));
            } else {
                BgOpLogPartition oplogPartition = bgOplog.get(tableId);
                oplogPartition
                        .serializeByServer(tableServerMemMap.get(tableId));
            }
        }
    }

    protected abstract BgOpLog prepareOpLogsToSend();

    protected void updateExistingRow(int tableId, int rowId,
            ClientRow clientRow, ClientTable clientTable, ByteBuffer data,
            int rowSize, int version) {
        Row rowData = clientRow.getRowDataPtr();

        Oplog tableOplog = clientTable.getOplog();
        tableOplog.lockRow(rowId);
        try {
            RowUpdate rowOplog = tableOplog.getRowUpdate(rowId);

            data.rewind();
            rowData.deserialize(data);

            boolean noOplogReplay = clientTable.isNoOplogReplay();

            if (!noOplogReplay)
                checkAndApplyOldOpLogsToRowData(tableId, rowId, version,
                        rowData);

            if (rowOplog != null && !noOplogReplay) {
                rowData.applyRowUpdate(rowOplog);
            }
        } finally {
            tableOplog.unlockRow(rowId);
        }
    }

    protected void insertNonexistentRow(int tableId, int rowId,
            ClientTable clientTable, ByteBuffer data, int rowSize, int version,
            int clock) {
        Row rowData = clientTable.createRow();

        rowData.deserialize(data);

        boolean noOplogReplay = clientTable.isNoOplogReplay();
        if (!noOplogReplay)
            checkAndApplyOldOpLogsToRowData(tableId, rowId, version, rowData);

        ClientRow clientRow = createClientRow(clock, rowData);

        Oplog tableOplog = clientTable.getOplog();
        tableOplog.lockRow(rowId);
        try {
            RowUpdate rowOplog = tableOplog.getRowUpdate(rowId);

            if (rowOplog != null && !noOplogReplay) {
                rowData.applyRowUpdate(rowOplog);
            }
            clientTable.getProcessStorage().putRow(rowId, clientRow);
        } finally {
            tableOplog.unlockRow(rowId);
        }
    }

    protected abstract void checkAndApplyOldOpLogsToRowData(int tableId,
            int rowId, int version, Row rowData);

    protected abstract ClientRow createClientRow(int clock, Row rowData);

    void sendOpLogMsgs(boolean clockAdvanced) {
        for (Integer serverId : this.serverIds) {
            ClientSendOpLogMsg oplogMsg = null;
            if (this.serverOplogMsgMap.containsKey(serverId)) {
                oplogMsg = this.serverOplogMsgMap.get(serverId);
            }
            if (oplogMsg != null) {
                oplogMsg.setIsClock(clockAdvanced);
                oplogMsg.setClientId(GlobalContext.getClientId());
                oplogMsg.setVersion(this.version);
                oplogMsg.setBgClock(this.clockHasPushed + 1);

                commBus.send(serverId, oplogMsg);
                this.serverOplogMsgMap.put(serverId, null);
            } else {
                ClientSendOpLogMsg clockOplogMsg = new ClientSendOpLogMsg(0);
                clockOplogMsg.setIsClock(clockAdvanced);
                clockOplogMsg.setClientId(GlobalContext.getClientId());
                clockOplogMsg.setVersion(this.version);
                clockOplogMsg.setBgClock(this.clockHasPushed + 1);

                commBus.send(serverId, clockOplogMsg);
            }
        }
    }

    protected int countRowOpLogToSend(int rowId, RowUpdate rowOplog,
            TIntIntMap tableNumBytesByServer, BgOpLogPartition bgTableOplog) {

        // update oplog message size
        int serverId = GlobalContext.getPartitionServerId(rowId,
                myCommChannelIdx);
        // 1) row id
        // 2) serialized row size
        int serializedSize = Integer.SIZE / Byte.SIZE
                + rowOplog.getSerializedSize();
        tableNumBytesByServer.adjustValue(serverId, serializedSize);
        bgTableOplog.insertOpLog(rowId, rowOplog);
        return serializedSize;
    }

    protected void finalizeOpLogMsgStats(int tableId,
            TIntIntMap tableNumBytesByServer2,
            TIntObjectMap<TIntIntMap> serverTableOplogSizeMap) {
        // 1. Integer.BYTES: number of rows
        for (TIntIntIterator serverIter = tableNumBytesByServer2.iterator(); serverIter
                .hasNext();) {
            serverIter.advance();
            if (serverIter.value() != 0)
                tableNumBytesByServer2.adjustValue(serverIter.key(),
                        Integer.SIZE / Byte.SIZE);
        }

        for (TIntIntIterator serverIter = tableNumBytesByServer2.iterator(); serverIter
                .hasNext();) {
            serverIter.advance();
            if (serverIter.value() == 0)
                serverTableOplogSizeMap.get(serverIter.key()).remove(tableId);
            else
                serverTableOplogSizeMap.get(serverIter.key()).put(tableId,
                        serverIter.value());
        }
    }

    public void run() {
        this.initWhenStart();

        ThreadContext.registerThread(this.myId);

        this.initCommBus();

        this.bgServerHandshake();

        try {
            this.initBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        IntBox numConnectedAppThreads = new IntBox(0);
        int numDeregisteredAppThreads = 0;
        int numShutdownAckedServers = 0;
        this.recvAppInitThreadConnection(numConnectedAppThreads);

        if (this.myCommChannelIdx == 0) {
            this.handleCreateTables();
        }
        try {
            this.createTableBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        this.finalizeTableStats();

        long timeoutMilli = GlobalContext.getBgIdleMilli();
        this.prepareBeforeInfiniteLoop();
        // System.out.println("Run till here before the worker thread infinit loop");
        while (true) {
            Msg msg = this.waitMsg.invoke(timeoutMilli);
            int senderId = msg.getSender();
            int msgType = msg.getMsgType();

            if (msg == null) {
                timeoutMilli = this.bgIdleWork();
                continue;
            } else {
                timeoutMilli = this.resetBgIdleMilli();
            }

            // System.out.println(myId + " received " + msgType + " from " +
            // senderId.intValue);

            switch (msgType) {
            case MsgType.APP_CONNECT:
                // System.out.println("AbstractBgWorker K_APP_CONNECT");
                ++numConnectedAppThreads.intValue;
                assert(numConnectedAppThreads.intValue <= GlobalContext.getNumAppThreads());
                break;
            case MsgType.APP_THREAD_DEREG:
                // System.out.println("AbstractBgWorker K_APP_THREAD_DEREG");
                ++numDeregisteredAppThreads;
                if (numDeregisteredAppThreads == GlobalContext
                        .getNumAppThreads()) {
                    Msg clientShutDownMsg = new Msg(MsgType.CLIENT_SHUT_DOWN);
                    int nameNodeId = GlobalContext.getNameNodeId();
                    commBus.send(nameNodeId, clientShutDownMsg);

                    for (Integer serverId : this.serverIds) {
                        commBus.send(serverId, clientShutDownMsg);
                    }
                }
                break;
            case MsgType.SERVER_SHUT_DOWN_ACK:
                // System.out.println("AbstractBgWorker K_SERVER_SHUT_DOWN_ACK");
                ++numShutdownAckedServers;
                if (numShutdownAckedServers == GlobalContext.getNumClients() + 1) {
                    commBus.threadDeregister();
                    return;
                }
                break;
            case MsgType.ROW_REQUEST:
                // System.out.println("AbstractBgWorker K_ROW_REQUEST");
                RowRequestMsg rowRequestMsg = RowRequestMsg.wrap(msg);
                this.checkForwardRowRequestToServer(senderId, rowRequestMsg);
                break;
            case MsgType.SERVER_ROW_REQUEST_REPLY:
                // System.out.println("AbstractBgWorker K_SERVER_ROW_REQUEST_REPLY");
                ServerRowRequestReplyMsg serverRowRequestReplyMsg = ServerRowRequestReplyMsg.wrap(msg);
                handleServerRowRequestReply(senderId, serverRowRequestReplyMsg);
                break;
            case MsgType.BG_CLOCK:
                // System.out.println("AbstractBgWorker K_BG_CLOCK");
                timeoutMilli = handleClockMsg(true);
                ++this.clientClock;
                break;
            case MsgType.BG_SEND_OP_LOG:
                // System.out.println("AbstractBgWorker K_BG_SEND_OP_LOG");
                timeoutMilli = handleClockMsg(false);
                break;
            case MsgType.SERVER_PUSH_ROW:
                // System.out.println("AbstractBgWorker K_SERVER_PUSH_ROW");
                // TODO: implement SSPPush
                // handleServerPushRow(senderId.intValue, msgMem);
                break;
            case MsgType.SERVER_OP_LOG_ACK:
                // System.out.println("AbstractBgWorker K_SERVER_OP_LOG_ACK");
                ServerOpLogAckMsg serverOplogAckMsg = ServerOpLogAckMsg.wrap(msg);
                rowRequestOplogMgr.serverAcknowledgeVersion(senderId,
                        serverOplogAckMsg.getAckVersion());
                break;
            case MsgType.BG_HANDLE_APPEND_OP_LOG:
                // System.out.println("AbstractBgWorker K_BG_HANDLE_APPEND_OP_LOG");
                // TODO: implement append-only
                // BgHandleAppendOpLogMsg handleAppendOplogMsg = new
                // BgHandleAppendOpLogMsg(
                // msgMem);
                // handleAppendOpLogMsg(handleAppendOplogMsg.getTableId());
                break;
            default:
                System.err.println("UNRECOGNIZED MESSAGE!!!" + msgType);
            }
        }
    }

    protected class WaitMsgBusy extends WaitMsgTimeOutFunc {
        @Override
        public Msg invoke(long timeoutMilli) {
            Msg msg = GlobalContext.commBus.recvAsync();
            while (msg == null) {
                msg = GlobalContext.commBus.recvAsync();
            }
            return msg;
        }
    }

    protected class WaitMsgSleep extends WaitMsgTimeOutFunc {
        @Override
        public Msg invoke(long timeoutMilli) {
            return GlobalContext.commBus.recv();
        }
    }
}
