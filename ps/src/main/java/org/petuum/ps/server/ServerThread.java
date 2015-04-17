package org.petuum.ps.server;

import org.petuum.ps.client.thread.GlobalContext;
import org.petuum.ps.client.thread.ThreadContext;
import org.petuum.ps.common.network.CommBus;
import org.petuum.ps.common.msg.*;
import org.petuum.ps.common.network.Msg;
import org.petuum.ps.common.util.IntBox;
import org.petuum.ps.common.util.PtrBox;
import org.petuum.ps.config.Config;
import org.petuum.ps.config.HostInfo;
import org.petuum.ps.config.TableInfo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class ServerThread extends Thread {

    protected CommBus.WaitMsgTimeOutFunc WaitMsg_;
    private int my_id_;
    private int[] bg_worker_ids_;
    private Server serverObj;
    private int num_shutdown_bgs_;
    private CommBus commBus;
    private CyclicBarrier init_barrier_;

    public ServerThread(int my_id, CyclicBarrier init_barrier) {
        this.my_id_ = my_id;
        this.bg_worker_ids_ = new int[GlobalContext.getNumClients()];
        this.num_shutdown_bgs_ = 0;
        this.commBus = GlobalContext.commBus;
        this.init_barrier_ = init_barrier;
        this.serverObj = new Server();
    }

    public void shutDown() {
        try {
            join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void SetWaitMsg() {

        if (GlobalContext.getAggressiveCpu()) {
            WaitMsg_ = new WaitMsgBusy();
        } else {
            WaitMsg_ = new WaitMsgSleep();
        }

    }

    public void InitWhenStart() {
        SetWaitMsg();
    }

    public void ServerPushRow(boolean clock_changed) {
    }

    public void RowSubscribe(ServerRow server_row, int client_id) {
    }

    public void SetUpCommBus() {
        CommBus.Config comm_config = new CommBus.Config();
        comm_config.entityId = my_id_;
        if (GlobalContext.getNumClients() > 0) {
            comm_config.lType = CommBus.K_IN_PROC | CommBus.K_INTER_PROC;
            HostInfo host_info = GlobalContext.getServerInfo(my_id_);
            comm_config.networkAddr = host_info.ip + ":" + host_info.port;
        } else {
            comm_config.lType = CommBus.K_IN_PROC;
        }

        commBus.threadRegister(comm_config);
    }

    public void ConnectToNameNode() {
        int name_node_id = GlobalContext.getNameNodeId();
        Msg server_connect_msg = new Msg(MsgType.SERVER_CONNECT);
        
        if (commBus.isLocalEntity(name_node_id)) {
            commBus.connectTo(name_node_id, server_connect_msg);
        } else {
            HostInfo name_node_info = GlobalContext.getNameNodeInfo();
            String name_node_addr = name_node_info.ip + ":"
                    + name_node_info.port;
            commBus.connectTo(name_node_id, name_node_addr, server_connect_msg);
        }
    }

    public int GetConnection(PtrBox<Boolean> is_client, IntBox client_id) {
        Msg msg = commBus.recv();

        if (msg.getMsgType() == MsgType.CLIENT_CONNECT) {
            ClientConnectMsg client_connect_msg = ClientConnectMsg.wrap(msg);
            is_client.value = true;
            client_id.intValue = client_connect_msg.getClientId();
        } else {
            is_client.value = false;
        }
        return msg.getSender();
    }

    public void SendToAllBgThreads(Msg msg) {
        for (int bg_worker_id : bg_worker_ids_) {
            commBus.send(bg_worker_id, msg);
        }

    }

    public void InitServer() {
        ConnectToNameNode();

        int num_bgs;
        for (num_bgs = 0; num_bgs < GlobalContext.getNumClients(); ++num_bgs) {
            IntBox client_id = new IntBox(0);
            PtrBox<Boolean> is_client = new PtrBox<>();

            int bg_id = GetConnection(is_client, client_id);
            bg_worker_ids_[num_bgs] = bg_id;
        }

        serverObj.init(my_id_, bg_worker_ids_);

        SendToAllBgThreads(new Msg(MsgType.CLIENT_START));
    }

    public boolean HandleShutDownMsg() {

        // When num_shutdown_bgs reaches the total number of clients, the server
        // reply to each bg with a ShutDownReply message
        num_shutdown_bgs_++;
        if (num_shutdown_bgs_ == GlobalContext.getNumClients()) {
            Msg shut_down_ack_msg = new Msg(MsgType.SERVER_SHUT_DOWN_ACK);

            for (int i = 0; i < GlobalContext.getNumClients(); i++) {
                int bg_id = bg_worker_ids_[i];
                commBus.send(bg_id, shut_down_ack_msg);
            }
            return true;
        }
        return false;
    }

    public void handleCreateTable(int sender_id, CreateTableMsg create_table_msg) {

        int table_id = create_table_msg.getTableId();
        CreateTableReplyMsg create_table_reply_msg = new CreateTableReplyMsg();
        create_table_reply_msg.setTableId(table_id);
        commBus.send(sender_id, create_table_reply_msg);

        TableInfo table_info = new TableInfo();

        table_info.tableStaleness = create_table_msg.getStaleness();
        table_info.rowType = create_table_msg.getRowType();
        table_info.rowUpdateType = create_table_msg.getRowOplogType();
        Config rowConfig = create_table_msg.getRowConfig();
        Config rowUpdateConfig = create_table_msg.getRowUpdateConfig();
        serverObj.createTable(table_id, table_info, rowConfig, rowUpdateConfig);
    }

    public void handleRowRequest(int sender_id, RowRequestMsg row_request_msg) {
        int table_id = row_request_msg.getTableId();
        int row_id = row_request_msg.getRowId();
        int clock = row_request_msg.getClock();
        int server_clock = serverObj.getMinClock();

        if (server_clock < clock) {
            // not fresh enough, wait
            serverObj.addRowRequest(sender_id, table_id, row_id, clock);
            return;
        }

        int version = serverObj.getBgVersion(sender_id);

        ServerRow server_row = serverObj.findCreateRow(table_id, row_id);

        RowSubscribe(server_row, GlobalContext.threadIdToClientId(sender_id));

        ReplyRowRequest(sender_id, server_row, table_id, row_id, server_clock,
                version);
    }

    public void ReplyRowRequest(int bg_id, ServerRow server_row, int table_id,
                                int row_id, int server_clock, int version) {
        // System.out.println("replyRowRequest " + this + " bgId=" + bg_id +
        // " tableId=" + table_id + " rowId=" + row_id + " version=" + version +
        // " serverClock=" + server_clock);
        int row_size = server_row.SerializedSize();
        ServerRowRequestReplyMsg server_row_request_reply_msg = new ServerRowRequestReplyMsg();
        server_row_request_reply_msg.setTableId(table_id);
        server_row_request_reply_msg.setRowId(row_id);
        server_row_request_reply_msg.setClock(server_clock);
        server_row_request_reply_msg.setVersion(version);
        server_row_request_reply_msg.setRowSize(row_size);

        ByteBuffer buf = server_row.Serialize();
        buf.rewind();
        server_row_request_reply_msg.setRowData(buf);

        commBus.send(bg_id, server_row_request_reply_msg);
    }

    public void handleOpLogMsg(int senderId,
                               ClientSendOpLogMsg clientSendOpLogMsg) {
        boolean isClock = clientSendOpLogMsg.getIsClock();
        int version = clientSendOpLogMsg.getVersion();
        int bgClock = clientSendOpLogMsg.getBgClock();
//         System.out.println("handleOpLogMsg " + this + " senderId=" + senderId
//         + " version=" + version + " bgClock=" + bgClock);
        serverObj.applyOpLogUpdateVersion(clientSendOpLogMsg.getData(),
                clientSendOpLogMsg.getAvaiSize(), senderId, version);

        boolean clock_changed = false;

        if (isClock) {
            // System.out.println("Server clock before = " +
            // serverObj.getMinClock());
            clock_changed = serverObj.clockUntil(senderId, bgClock);
            // System.out.println("Server clock after = " +
            // serverObj.getMinClock());
            if (clock_changed) {
                ArrayList<ServerRowRequest> requests = new ArrayList<>();
                serverObj.getFulfilledRowRequests(requests);
                //System.out.println("reply row request sent " + requests.size());
                for (ServerRowRequest request : requests) {
                    int table_id = request.tableId;
                    int row_id = request.rowId;
                    int bg_id = request.bgId;
                    int bg_version = serverObj.getBgVersion(bg_id);
                    ServerRow server_row = serverObj.findCreateRow(table_id,
                            row_id);
                    RowSubscribe(server_row,
                            GlobalContext.threadIdToClientId(bg_id));
                    int server_clock = serverObj.getMinClock();

                    ReplyRowRequest(bg_id, server_row, table_id, row_id,
                            server_clock, bg_version);
                }

            }
        }

        //System.out.println("clock changed = " + clock_changed);
        if (clock_changed) {
            ServerPushRow(clock_changed);
        } else {
            SendOpLogAckMsg(senderId, serverObj.getBgVersion(senderId));

        }

    }

    public long ServerIdleWork() {
        return 0;
    }

    public long ResetServerIdleMilli() {
        return 0;
    }

    public void SendOpLogAckMsg(int bg_id, int version) {

    }

    @Override
    public void run() {
        this.InitWhenStart();
        ThreadContext.registerThread(my_id_);

        // NumaMgr::ConfigureServerThread();
        SetUpCommBus();

        try {
            init_barrier_.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }

        InitServer();
        long timeout_milli = GlobalContext.getServerIdleMilli();

        while (true) {
            Msg msg = WaitMsg_.invoke(timeout_milli);
            int senderId = msg.getSender();
            if (msg == null) {
                timeout_milli = ServerIdleWork();
                continue;
            } else {
                timeout_milli = GlobalContext.getServerIdleMilli();
            }

            int msgType = msg.getMsgType();

            switch (msgType) {
                case MsgType.CLIENT_SHUT_DOWN:
                    // System.out.println("ServerThread K_CLIENT_SHUT_DOWN");
                    boolean shutdown = HandleShutDownMsg();
                    if (shutdown) {
                        commBus.threadDeregister();
                        return;
                    }
                    break;

                case MsgType.CREATE_TABLE:
                    // System.out.println("ServerThread K_CREATE_TABLE");
                    CreateTableMsg createTableMsg = CreateTableMsg.wrap(msg);
                    handleCreateTable(senderId, createTableMsg);
                    break;
                case MsgType.ROW_REQUEST:
                    //System.out.println("ServerThread K_ROW_REQUEST");
                    RowRequestMsg rowRequestMsg = RowRequestMsg.wrap(msg);
                    handleRowRequest(senderId, rowRequestMsg);
                    break;
                case MsgType.CLIENT_SEND_OP_LOG:
                    //System.out.println("ServerThread K_CLIENT_SEND_OP_LOG");
                    ClientSendOpLogMsg clientSendOpLogMsg = ClientSendOpLogMsg.wrap(msg);
                    handleOpLogMsg(senderId, clientSendOpLogMsg);
                    //System.out.println("Client Send OpLog returned");
                    break;
                default:
                    System.out.println("FATAL");
                    return;
            }
        }

    }

    protected class WaitMsgBusy extends CommBus.WaitMsgTimeOutFunc {

        @Override
        public Msg invoke(long timeoutMilli) {
            Msg msg = commBus.recvAsync();
            while (msg == null) {
                msg = commBus.recv();
            }
            return msg;
        }
    }

    protected class WaitMsgSleep extends CommBus.WaitMsgTimeOutFunc {

        @Override
        public Msg invoke(long timeoutMilli) {
            return commBus.recv();
        }

    }

    protected class WaitMsgTimeOut extends CommBus.WaitMsgTimeOutFunc {

        @Override
        public Msg invoke(long timeoutMilli) {
            return commBus.recv(timeoutMilli);
        }

    }
}
