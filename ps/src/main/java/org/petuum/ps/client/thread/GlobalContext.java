package org.petuum.ps.client.thread;

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.petuum.ps.common.network.CommBus;
import org.petuum.ps.config.HostInfo;

import java.util.ArrayList;

public class GlobalContext {
    public static final int K_MAX_NUM_THREADS_PER_CLIENT = 1000;
    public static final int K_BG_THREAD_ID_START_OFFSET = 100;
    public static final int K_INIT_THREAD_ID_OFFSET = 200;
    public static final int K_SERVER_THREAD_ID_START_OFFSET = 1;
    public static CommBus commBus;
    private static int numClients;

    private static int numCommChannelsPerClient;
    private static int numTotalCommChannels;

    private static int numAppThreads;
    private static int numTableThreads;
    private static int numTables;

    private static TIntObjectMap<HostInfo> hostMap;
    private static TIntObjectMap<HostInfo> serverMap;
    private static HostInfo nameNodeHostInfo;
    private static ArrayList<Integer> serverIds;

    private static int clientId;
    private static int serverRingSize;
    private static int consistencyModel;
    private static int localIdMin;
    private static boolean aggressiveCpu;

    private static int snapshotClock;
    private static String snapshotDir;
    private static int resumeClock;
    private static String resumeDir;
    private static long bgIdleMilli;

    private static int oplogPushUpperBoundKb;
    private static int oplogPushStalenessTolerance;

    private static int threadOplogBatchSize;
    private static int serverOplogPushBatchSize;

    private static int serverPushRowThreshold;

    private static long serverIdleMilli;

    public static int getThreadIdMin(int clientId) {
        return clientId * K_MAX_NUM_THREADS_PER_CLIENT;
    }

    public static int getThreadIdMax(int clientId) {
        return (clientId + 1) * K_MAX_NUM_THREADS_PER_CLIENT - 1;
    }

    public static int getNameNodeId() {
        return 0;
    }

    public static int getNameNodeClientId() {
        return 0;
    }

    public static boolean amINameNodeClient() {
        return clientId == getNameNodeClientId();
    }

    public static int getBgThreadId(int clientId, int commChannelIdx) {
        return getThreadIdMin(clientId) + K_BG_THREAD_ID_START_OFFSET
                + commChannelIdx;
    }

    public static int getHeadBgId(int clientId) {
        return getBgThreadId(clientId, 0);
    }

    public static int getServerThreadId(int clientId, int commChannelIdx) {
        return getThreadIdMin(clientId) + K_SERVER_THREAD_ID_START_OFFSET
                + commChannelIdx;
    }

    public static ArrayList<Integer> getServerThreadIDs(int commChannelIdx) {
        ArrayList<Integer> serverThreadIds = new ArrayList<Integer>();
        for (int i = 0; i < GlobalContext.numClients; i++) {
            serverThreadIds.add(getServerThreadId(i, commChannelIdx));
        }
        return serverThreadIds;
    }

    public static int threadIdToClientId(int threadId) {
        return threadId / K_MAX_NUM_THREADS_PER_CLIENT;
    }

    public static int getSerializedTableSeparator() {
        return -1;
    }

    public static int getSerializedTableEnd() {
        return -2;
    }

    public static void init(int numCommChannelsPerClient, int numAppThreads,
                            int numTableThreads, int numTables, int numClients,
                            TIntObjectMap<HostInfo> host_map, int clientId, int serverRingSize,
                            int consistencyModel, boolean aggressiveCpu, int snapshotClock,
                            String snapshotDir, int resumeClock, String resumeDir,
                            long bgIdleMilli,
                            int oplogPushUpperBoundKb, int oplogPushStalenessTolerance,
                            int threadOplogBatchSize, int serverPushRowThreshold,
                            long serverIdleMilli) {

        GlobalContext.numCommChannelsPerClient = numCommChannelsPerClient;
        GlobalContext.numTotalCommChannels = numCommChannelsPerClient
                * numClients;

        GlobalContext.numAppThreads = numAppThreads;
        GlobalContext.numTableThreads = numTableThreads;
        GlobalContext.numTables = numTables;
        GlobalContext.numClients = numClients;
        GlobalContext.hostMap = host_map;

        GlobalContext.clientId = clientId;
        GlobalContext.serverRingSize = serverRingSize;
        GlobalContext.consistencyModel = consistencyModel;

        GlobalContext.localIdMin = getThreadIdMin(clientId);
        GlobalContext.aggressiveCpu = aggressiveCpu;

        GlobalContext.snapshotClock = snapshotClock;
        GlobalContext.snapshotDir = snapshotDir;
        GlobalContext.resumeClock = resumeClock;
        GlobalContext.bgIdleMilli = bgIdleMilli;

        GlobalContext.oplogPushUpperBoundKb = oplogPushUpperBoundKb;
        GlobalContext.oplogPushStalenessTolerance = oplogPushStalenessTolerance;
        GlobalContext.threadOplogBatchSize = threadOplogBatchSize;

        GlobalContext.serverPushRowThreshold = serverPushRowThreshold;

        GlobalContext.serverIdleMilli = serverIdleMilli;

        GlobalContext.serverMap = new TIntObjectHashMap<>();
        GlobalContext.serverIds = new ArrayList<>();

        for (TIntObjectIterator<HostInfo> entry = host_map.iterator(); entry
                .hasNext(); ) {
            entry.advance();
            HostInfo hostInfo = entry.value();
            int portNum = Integer.parseInt(hostInfo.port);

            if (entry.key() == getNameNodeId()) {
                GlobalContext.nameNodeHostInfo = hostInfo;

                portNum++;
                hostInfo = new HostInfo(hostInfo);
                hostInfo.port = Integer.toString(portNum);
            }

            for (int i = 0; i < GlobalContext.numCommChannelsPerClient; i++) {
                int serverId = getServerThreadId(entry.key(), i);
                GlobalContext.serverMap.put(serverId, hostInfo);

                portNum++;
                hostInfo = new HostInfo(hostInfo);
                hostInfo.port = Integer.toString(portNum);

                GlobalContext.serverIds.add(serverId);
            }
        }
    }

    public static int getNumTotalCommChannels() {
        return GlobalContext.numTotalCommChannels;
    }

    public static int getNumCommChannelsPerClient() {
        return GlobalContext.numCommChannelsPerClient;
    }

    public static int getNumAppThreads() {
        return GlobalContext.numAppThreads;
    }

    public static int getNumTableThreads() {
        return GlobalContext.numTableThreads;
    }

    public static int getHeadTableThreadId() {
        int initThreadId = getThreadIdMin(GlobalContext.clientId)
                + K_INIT_THREAD_ID_OFFSET;
        return (GlobalContext.numTableThreads == GlobalContext.numAppThreads) ? initThreadId
                : initThreadId + 1;
    }

    public static int getNumTables() {
        return GlobalContext.numTables;
    }

    public static int getNumClients() {
        return GlobalContext.numClients;
    }

    public static int getNumTotalServers() {
        return GlobalContext.numCommChannelsPerClient
                * GlobalContext.numClients;
    }

    public static HostInfo getServerInfo(int serverId) {
        return GlobalContext.serverMap.get(serverId);
    }

    public static HostInfo getNameNodeInfo() {
        return GlobalContext.nameNodeHostInfo;
    }

    public static ArrayList<Integer> getAllServerIds() {
        return GlobalContext.serverIds;
    }

    public static int getClientId() {
        return GlobalContext.clientId;
    }

    public static int getPartitionCommChannelIndex(int rowId) {
        return rowId % GlobalContext.numCommChannelsPerClient;
    }

    public static int getPartitionClientId(int rowId) {
        return (rowId / numCommChannelsPerClient) % numClients;
    }

    public static int getPartitionServerId(int rowId, int commChannelIdx) {
        int clientId = getPartitionClientId(rowId);
        return getServerThreadId(clientId, commChannelIdx);
    }

    public static int getCommChannelIndexServer(int serverId) {
        return serverId % K_MAX_NUM_THREADS_PER_CLIENT
                - K_SERVER_THREAD_ID_START_OFFSET;
    }

    public static int getServerRingSize() {
        return serverRingSize;
    }

    public static int getConsistencyModel() {
        return consistencyModel;
    }

    public static int getLocalIdMin() {
        return localIdMin;
    }

    public static boolean getAggressiveCpu() {
        return aggressiveCpu;
    }

    public static int getLockPoolSize() {
        final int K_STRIPED_LOCK_EXPANSION_FACTOR = 100;
        return (numAppThreads + numCommChannelsPerClient)
                * K_STRIPED_LOCK_EXPANSION_FACTOR;
    }

    public static int getLockPoolSize(int tableCapacity) {
        final int K_STRIPED_LOCK_REDUCTION_FACTOR = 1;
        return (tableCapacity <= 2 * K_STRIPED_LOCK_REDUCTION_FACTOR) ? tableCapacity
                : tableCapacity / K_STRIPED_LOCK_REDUCTION_FACTOR;
    }

    public static int getSnapShotClock() {
        return snapshotClock;
    }

    public static String getSnapShotDir() {
        return snapshotDir;
    }

    public static int getResumeClock() {
        return resumeClock;
    }

    public static String getResumeDir() {
        return resumeDir;
    }

    public static long getBgIdleMilli() {
        return bgIdleMilli;
    }

    public static int getOplogPushUpperBoundKb() {
        return oplogPushUpperBoundKb;
    }

    public static int getOplogPushStalenessTolerance() {
        return oplogPushStalenessTolerance;
    }

    public static int getThreadOplogBatchSize() {
        return threadOplogBatchSize;
    }

    public static int getServerPushRowThreshold() {
        return serverPushRowThreshold;
    }

    public static long getServerIdleMilli() {
        return serverIdleMilli;
    }

}
