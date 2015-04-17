package org.petuum.ps.server;

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
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
import java.util.LinkedList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NameNodeThread extends Thread {
    private static final Logger logger =
        LoggerFactory.getLogger(NameNodeThread.class);

    private int my_id_;
    private CyclicBarrier init_barrier;
    private CommBus comm_bus;
    private int[] bg_worker_ids;
    private TIntObjectMap<CreateTableInfo> create_table_map;
    private Server server_obj;
    private int num_shutdown_bgs;

    public NameNodeThread(CyclicBarrier init_barrier) {
        this.init_barrier = init_barrier;
        this.comm_bus = GlobalContext.commBus;
        this.bg_worker_ids = new int[GlobalContext.getNumClients()
                * GlobalContext.getNumCommChannelsPerClient()];
        this.num_shutdown_bgs = 0;
        this.server_obj = new Server();
        this.create_table_map = new TIntObjectHashMap<>();
    }

    public void shutDown() {

    }

    // communication function
    public int GetConnection(PtrBox<Boolean> is_client,
                             PtrBox<Integer> client_id) {
        Msg msg = comm_bus.recv();
        int sender_id = msg.getSender();
        int msg_type = msg.getMsgType();
        if (msg_type == MsgType.CLIENT_CONNECT) {
            ClientConnectMsg clientConnectMsg = ClientConnectMsg.wrap(msg);
            is_client.value = true;
            client_id.value = clientConnectMsg.getClientId();
        } else {
            is_client.value = false;
        }
        return sender_id;

    }

    public void SendToAllServers(Msg msg) {
        ArrayList<Integer> server_ids = GlobalContext.getAllServerIds();
        for (int server_id : server_ids) {
            boolean is_sent = comm_bus.send(server_id, msg);
        }
    }

    public void SendToAllBgThreads(Msg msg) {
        for (int bg_id : this.bg_worker_ids) {
            boolean is_sent = comm_bus.send(bg_id, msg);
        }
    }

    public void SetUpNameNodeContext() {

    }

    public void SetUpCommBus() {
        CommBus.Config comm_config = new CommBus.Config();
        comm_config.entityId = my_id_;
        if (GlobalContext.getNumClients() > 0) {
            comm_config.lType = CommBus.K_IN_PROC | CommBus.K_INTER_PROC;
            HostInfo host_info = GlobalContext.getNameNodeInfo();
            comm_config.networkAddr = host_info.ip + ":" + host_info.port;
        } else {
            comm_config.lType = CommBus.K_IN_PROC;
        }

        comm_bus.threadRegister(comm_config);
        logger.debug("NameNode is ready to accept connections!");
    }

    public void InitNameNode() {
        int num_bgs = 0;
        int num_servers = 0;
        int num_expected_conns = 2 * GlobalContext.getNumTotalCommChannels();

        for (int num_connections = 0; num_connections < num_expected_conns; ++num_connections) {
            PtrBox<Integer> client_id = new PtrBox<>();
            PtrBox<Boolean> is_client = new PtrBox<>();
            int sender_id = GetConnection(is_client, client_id);
            if (is_client.value) {
                bg_worker_ids[num_bgs] = sender_id;
                ++num_bgs;
            } else {
                ++num_servers;
            }
        }

        this.server_obj.init(0, bg_worker_ids);

        SendToAllBgThreads(new Msg(MsgType.CONNECT_SERVER));

        SendToAllBgThreads(new Msg(MsgType.CLIENT_START));

    }

    public boolean HaveCreatedAllTables() {

        if (create_table_map.size() < GlobalContext.getNumTables()) {
            return false;
        }

        TIntObjectIterator<CreateTableInfo> iter = create_table_map.iterator();
        while (iter.hasNext()) {
            iter.advance();
            if (!(iter.value().RepliedToAllClients())) {
                return false;
            }
        }

        return true;

    }

    public void SendCreatedAllTablesMsg() {
        Msg created_all_tables_msg = new Msg(MsgType.CREATED_ALL_TABLES);
        int num_clients = GlobalContext.getNumClients();
        for (int client_idx = 0; client_idx < num_clients; ++client_idx) {

            int head_bg_id = GlobalContext.getHeadBgId(client_idx);
            comm_bus.send(head_bg_id, created_all_tables_msg);
        }
    }

    public boolean HandleShutDownMsg() {
        // When num_shutdown_bgs reaches the total number of bg threads, the
        // server
        // reply to each bg with a ShutDownReply message
        ++num_shutdown_bgs;
        if (num_shutdown_bgs == GlobalContext.getNumTotalCommChannels()) {
            Msg shut_down_ack_msg = new Msg(MsgType.SERVER_SHUT_DOWN_ACK);
            for (int i = 0; i < GlobalContext.getNumTotalCommChannels(); ++i) {
                int bg_id = bg_worker_ids[i];
                comm_bus.send(bg_id, shut_down_ack_msg);
            }
            return true;
        }
        return false;
    }

    public void HandleCreateTable(int sender_id, CreateTableMsg create_table_msg) {
        int table_id = create_table_msg.getTableId();
        if (!create_table_map.containsKey(table_id)) {
            TableInfo table_info = new TableInfo();
            table_info.tableStaleness = create_table_msg.getStaleness();
            table_info.rowType = create_table_msg.getRowType();
            table_info.rowUpdateType = create_table_msg.getRowOplogType();
            Config rowConfig = create_table_msg.getRowConfig();
            Config rowUpdateConfig = create_table_msg.getRowUpdateConfig();
            server_obj.createTable(table_id, table_info, rowConfig, rowUpdateConfig);

            create_table_map.put(table_id, new CreateTableInfo());
            SendToAllServers(create_table_msg);
        }

        if (create_table_map.get(table_id).ReceivedFromAllServers()) {
            CreateTableReplyMsg create_table_reply_msg = new CreateTableReplyMsg();
            create_table_reply_msg.setTableId(create_table_msg.getTableId());
            boolean isSent = comm_bus.send(sender_id,
                    create_table_reply_msg);
            create_table_map.get(table_id).num_clients_replied_++;
            if (HaveCreatedAllTables())
                SendCreatedAllTablesMsg();
        } else {
            // to be sent later
            create_table_map.get(table_id).bgs_to_reply_.push(sender_id);
        }
    }

    public void HandleCreateTableReply(
            CreateTableReplyMsg create_table_reply_msg) {

        int table_id = create_table_reply_msg.getTableId();
        create_table_map.get(table_id).num_servers_replied_++;

        if (create_table_map.get(table_id).ReceivedFromAllServers()) {
            LinkedList<Integer> bgs_to_reply = create_table_map.get(table_id).bgs_to_reply_;
            while (!bgs_to_reply.isEmpty()) {
                int bg_id = bgs_to_reply.peekFirst();
                bgs_to_reply.pop();
                boolean isSend = comm_bus.send(bg_id,
                        create_table_reply_msg);
                ++create_table_map.get(table_id).num_clients_replied_;
            }
            if (HaveCreatedAllTables())
                SendCreatedAllTablesMsg();
        }
    }

    public void run() {
        ThreadContext.registerThread(my_id_);

        // set up thread-specific server context
        SetUpCommBus();
        try {
            init_barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        InitNameNode();
        while (true) {
            Msg msg = comm_bus.recv();
            int sender_id = msg.getSender();
            int msg_type = msg.getMsgType();

            switch (msg_type) {
                case MsgType.CLIENT_SHUT_DOWN: {
                    boolean shutdown = HandleShutDownMsg();
                    if (shutdown) {
                        comm_bus.threadDeregister();
                        return;
                    }
                    break;
                }
                case MsgType.CREATE_TABLE: {
                    CreateTableMsg create_table_msg = CreateTableMsg.wrap(msg);
                    HandleCreateTable(sender_id, create_table_msg);
                    break;
                }
                case MsgType.CREATE_TABLE_REPLY: {
                    CreateTableReplyMsg create_table_reply_msg = CreateTableReplyMsg.wrap(msg);
                    HandleCreateTableReply(create_table_reply_msg);
                    break;
                }
                default:
                    System.out.println("Unrecognized message type: " + msg_type);
            }

        }

    }

    private class CreateTableInfo {
        public int num_clients_replied_;
        public int num_servers_replied_;
        public LinkedList<Integer> bgs_to_reply_;

        public CreateTableInfo() {
            this.num_clients_replied_ = 0;
            this.num_servers_replied_ = 0;
            this.bgs_to_reply_ = new LinkedList<>();
        }

        public CreateTableInfo(CreateTableInfo info_obj) {
            this.num_clients_replied_ = info_obj.num_clients_replied_;
            this.num_servers_replied_ = info_obj.num_servers_replied_;
            this.bgs_to_reply_ = info_obj.bgs_to_reply_;
        }

        public boolean ReceivedFromAllServers() {
            return (num_servers_replied_ == GlobalContext.getNumTotalServers());
        }

        public boolean RepliedToAllClients() {
            return (num_clients_replied_ == GlobalContext.getNumClients());
        }
    }

}
