package org.petuum.ps.client;

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.petuum.ps.client.thread.BgWorkers;
import org.petuum.ps.client.thread.GlobalContext;
import org.petuum.ps.client.thread.ThreadContext;
import org.petuum.ps.common.network.CommBus;
import org.petuum.ps.common.network.NettyCommBus;
import org.petuum.ps.common.util.IntBox;
import org.petuum.ps.common.util.VectorClockMT;
import org.petuum.ps.config.HostInfo;
import org.petuum.ps.config.TableConfig;
import org.petuum.ps.config.TableGroupConfig;
import org.petuum.ps.server.NameNode;
import org.petuum.ps.server.ServerThreads;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yuxin Su
 * @version 1.0
 * @created 19-??-2014 15:26:05
 */
public class TableGroup extends AbstractTableGroup {
    private static final Logger logger =
        LoggerFactory.getLogger(TableGroup.class);

    /**
     * Max staleness among all tables.
     */
    private int maxTableStaleness;
    private AtomicInteger numAppThreadsRegistered = new AtomicInteger();
    private TIntObjectMap<ClientTable> tables = new TIntObjectHashMap<>();
    private VectorClockMT vectorClock;
    private Method clockInternal;
    private CyclicBarrier registerBarrier;

    /**
     * @param tableGroupConfig
     * @param tableAccess
     */
    public TableGroup(final TableGroupConfig tableGroupConfig,
                      boolean tableAccess, IntBox initThreadID) {
        this.vectorClock = new VectorClockMT();
        int num_comm_channels_per_client = tableGroupConfig.getNumCommChannelsPerClient();
        int num_local_app_threads = tableGroupConfig.getNumLocalAppThreads();
        int num_local_table_threads = tableAccess ? num_local_app_threads
                : (num_local_app_threads - 1);

        int num_tables = tableGroupConfig.getNumTables();
        int num_total_clients = tableGroupConfig.getNumTotalClients();
        Map<Integer, HostInfo> host_map = tableGroupConfig.getHostMap();

        int client_id = tableGroupConfig.getClientId();
        int server_ring_size = tableGroupConfig.getServerRingSize();
        int localIDMin = GlobalContext
                .getThreadIdMin(tableGroupConfig.getClientId());
        int localIDMax = GlobalContext
                .getThreadIdMax(tableGroupConfig.getClientId());
        int consistency_model = tableGroupConfig.getConsistencyModel();
        numAppThreadsRegistered.set(1); // init thread is the first one
        TIntObjectMap<HostInfo> tHostMap = new TIntObjectHashMap<>();
        for (Map.Entry<Integer, HostInfo> e : host_map.entrySet()) {
            tHostMap.put(e.getKey(), e.getValue());
        }

        GlobalContext.init(num_comm_channels_per_client, num_local_app_threads,
                num_local_table_threads, num_tables, num_total_clients,
                tHostMap, client_id, server_ring_size, consistency_model,
                tableGroupConfig.isAggressiveCpu(),
                tableGroupConfig.getSnapshotClock(),
                tableGroupConfig.getSnapshotDir(),
                tableGroupConfig.getResumeClock(),
                tableGroupConfig.getResumeDir(),
                tableGroupConfig.getBgIdleMilli(),
                tableGroupConfig.getOplogPushUpperBoundKb(),
                tableGroupConfig.getOplogPushStalenessTolerance(),
                tableGroupConfig.getThreadOplogBatchSize(),
                tableGroupConfig.getServerPushRowThreshold(),
                tableGroupConfig.getServerIdleMilli());

        // TODO: injection point
        GlobalContext.commBus = new NettyCommBus(localIDMin, localIDMax,
                num_total_clients, 1);

        initThreadID.intValue = localIDMin
                + GlobalContext.K_INIT_THREAD_ID_OFFSET;

        CommBus.Config comm_config = new CommBus.Config(initThreadID.intValue,
                CommBus.K_NONE, "");
        GlobalContext.commBus.threadRegister(comm_config);
        ThreadContext.registerThread(initThreadID.intValue);
        logger.debug("CommBus is finished setting up");

        if (GlobalContext.amINameNodeClient()) {
            NameNode.init();
            ServerThreads.init(localIDMin + 1);
        } else {
            ServerThreads.init(localIDMin);
        }

        logger.debug("Server thread is ready");

        BgWorkers.start(tables);
        BgWorkers.appThreadRegister();

        if (tableAccess) {
            vectorClock.addClock(initThreadID.intValue, 0);
        }

        if (tableGroupConfig.isAggressiveClock()) {
            try {
                clockInternal = TableGroup.class.getMethod("clockAggressive");
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            }
        } else {
            try {
                clockInternal = TableGroup.class.getMethod("clockConservative");
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            }
        }
        logger.debug("Finished setting up table group");
    }

    public void close() {
        BgWorkers.appThreadDeregister();
        ServerThreads.shutdown();
        if (GlobalContext.amINameNodeClient()) {
            NameNode.shutdown();
        }
        BgWorkers.shutdown();
        GlobalContext.commBus.threadDeregister();
        GlobalContext.commBus.close();
        for (TIntObjectIterator<ClientTable> iter = tables.iterator(); iter
                .hasNext(); ) {
            iter.advance();
            iter.value().close();
        }
    }

    public boolean createTable(int tableId, final TableConfig tableConfig) {
        if (tableConfig.getStaleness() > maxTableStaleness) {
            maxTableStaleness = tableConfig.getStaleness();
        }

        boolean suc = BgWorkers.createTable(tableId, tableConfig);
        if (suc
                && (GlobalContext.getNumAppThreads() == GlobalContext
                .getNumTableThreads())) {
            tables.get(tableId).registerThread();
        }
        return suc;
    }

    public void createTableDone() {

        BgWorkers.waitCreateTable();
        registerBarrier = new CyclicBarrier(GlobalContext.getNumTableThreads());

    }

    public void waitThreadRegister() {

        if (GlobalContext.getNumTableThreads() == GlobalContext
                .getNumAppThreads()) {
            try {
                registerBarrier.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
        }

    }

    public ClientTable getTableOrDie(int tableId) {
        return tables.get(tableId);
    }

    public int registerThread() {
        int appThreadIdOffset = numAppThreadsRegistered.getAndIncrement();

        int threadId = GlobalContext.getLocalIdMin()
                + GlobalContext.K_INIT_THREAD_ID_OFFSET + appThreadIdOffset;

        CommBus.Config config = new CommBus.Config(threadId, CommBus.K_NONE, "");

        ThreadContext.registerThread(threadId);

        GlobalContext.commBus.threadRegister(config);

        BgWorkers.appThreadRegister();
        vectorClock.addClock(threadId, 0);
        for (TIntObjectIterator<ClientTable> iter = tables.iterator(); iter
                .hasNext(); ) {
            iter.advance();
            iter.value().registerThread();
        }

        try {
            registerBarrier.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
        return threadId;
    }

    public void deregisterThread() {
        for (TIntObjectIterator<ClientTable> iter = tables.iterator(); iter
                .hasNext(); ) {
            iter.advance();
            iter.value().deregisterThread();
        }

        BgWorkers.appThreadDeregister();
        GlobalContext.commBus.threadDeregister();
    }

    public void clock() {
        ThreadContext.clock();
        try {
            clockInternal.invoke(TableGroup.this);
        } catch (IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    public void globalBarrier() {
        for (int i = 0; i < maxTableStaleness + 1; i++) {
            clock();
        }
    }

    public void clockAggressive() {
        for (TIntObjectIterator<ClientTable> iter = tables.iterator(); iter
                .hasNext(); ) {
            iter.advance();
            iter.value().clock();
        }

        int clock = vectorClock.tick(ThreadContext.getId());
        if (clock != 0) {
            BgWorkers.clockAllTables();
        } else {
            BgWorkers.sendOpLogsAllTables();
        }
    }

    public void clockConservative() {
        for (TIntObjectIterator<ClientTable> iter = tables.iterator(); iter
                .hasNext(); ) {
            iter.advance();
            iter.value().clock();
        }

        int clock = vectorClock.tick(ThreadContext.getId());
        if (clock != 0) {
            BgWorkers.clockAllTables();
        }
    }

}
