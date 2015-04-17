package org.petuum.ps.config;

import org.petuum.ps.common.util.Utils;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by aqiao on 3/5/15.
 */
public class TableGroupConfig {

    private int numCommChannelsPerClient;
    private int numTables;
    private int numTotalClients;
    private int numLocalAppThreads;
    private Map<Integer, HostInfo> hostMap;
    private int clientId;
    private boolean aggressiveClock;
    private int consistencyModel;
    private boolean aggressiveCpu;
    private int serverRingSize;
    private int snapshotClock;
    private int resumeClock;
    private String snapshotDir;
    private String resumeDir;
    private int oplogPushUpperBoundKb;
    private int oplogPushStalenessTolerance;
    private int threadOplogBatchSize;
    private int serverPushRowThreshold;
    private long bgIdleMilli;
    private long serverIdleMilli;

    public TableGroupConfig() {
        numCommChannelsPerClient = 1;
        numTables = 1;
        numTotalClients = 1;
        numLocalAppThreads = 1;
        aggressiveClock = false;
        aggressiveCpu = false;
        snapshotClock = -1;
        resumeClock = -1;
        bgIdleMilli = 2;
        serverIdleMilli = 2;
        oplogPushUpperBoundKb = 100;
        oplogPushStalenessTolerance = 2;
        threadOplogBatchSize = 100 * 1000 * 1000;
        consistencyModel = ConsistencyModel.SSP;
        hostMap = new HashMap<Integer, HostInfo>();
    }

    public TableGroupConfig(String hostFile) {
        this();
        Utils.getHostInfos(hostFile, hostMap);
        numTotalClients = hostMap.size();
    }

    public int getNumCommChannelsPerClient() {
        return numCommChannelsPerClient;
    }

    public TableGroupConfig setNumCommChannelsPerClient(int numCommChannelsPerClient) {
        this.numCommChannelsPerClient = numCommChannelsPerClient;
        return this;
    }

    public int getNumTables() {
        return numTables;
    }

    public TableGroupConfig setNumTables(int numTables) {
        this.numTables = numTables;
        return this;
    }

    public int getNumTotalClients() {
        return numTotalClients;
    }

    public TableGroupConfig setNumTotalClients(int numTotalClients) {
        this.numTotalClients = numTotalClients;
        return this;
    }

    public int getNumLocalAppThreads() {
        return numLocalAppThreads;
    }

    public TableGroupConfig setNumLocalAppThreads(int numLocalAppThreads) {
        this.numLocalAppThreads = numLocalAppThreads;
        return this;
    }

    public Map<Integer, HostInfo> getHostMap() {
        return hostMap;
    }

    public TableGroupConfig setHostMap(Map<Integer, HostInfo> hostMap) {
        this.hostMap = hostMap;
        return this;
    }

    public int getClientId() {
        return clientId;
    }

    public TableGroupConfig setClientId(int clientId) {
        this.clientId = clientId;
        return this;
    }

    public boolean isAggressiveClock() {
        return aggressiveClock;
    }

    public TableGroupConfig setAggressiveClock(boolean aggressiveClock) {
        this.aggressiveClock = aggressiveClock;
        return this;
    }

    public int getConsistencyModel() {
        return consistencyModel;
    }

    public TableGroupConfig setConsistencyModel(int consistencyModel) {
        this.consistencyModel = consistencyModel;
        return this;
    }

    public boolean isAggressiveCpu() {
        return aggressiveCpu;
    }

    public TableGroupConfig setAggressiveCpu(boolean aggressiveCpu) {
        this.aggressiveCpu = aggressiveCpu;
        return this;
    }

    public int getServerRingSize() {
        return serverRingSize;
    }

    public TableGroupConfig setServerRingSize(int serverRingSize) {
        this.serverRingSize = serverRingSize;
        return this;
    }

    public int getSnapshotClock() {
        return snapshotClock;
    }

    public TableGroupConfig setSnapshotClock(int snapshotClock) {
        this.snapshotClock = snapshotClock;
        return this;
    }

    public int getResumeClock() {
        return resumeClock;
    }

    public TableGroupConfig setResumeClock(int resumeClock) {
        this.resumeClock = resumeClock;
        return this;
    }

    public String getSnapshotDir() {
        return snapshotDir;
    }

    public TableGroupConfig setSnapshotDir(String snapshotDir) {
        this.snapshotDir = snapshotDir;
        return this;
    }

    public String getResumeDir() {
        return resumeDir;
    }

    public TableGroupConfig setResumeDir(String resumeDir) {
        this.resumeDir = resumeDir;
        return this;
    }

    public int getOplogPushUpperBoundKb() {
        return oplogPushUpperBoundKb;
    }

    public TableGroupConfig setOplogPushUpperBoundKb(int oplogPushUpperBoundKb) {
        this.oplogPushUpperBoundKb = oplogPushUpperBoundKb;
        return this;
    }

    public int getOplogPushStalenessTolerance() {
        return oplogPushStalenessTolerance;
    }

    public TableGroupConfig setOplogPushStalenessTolerance(int oplogPushStalenessTolerance) {
        this.oplogPushStalenessTolerance = oplogPushStalenessTolerance;
        return this;
    }

    public int getThreadOplogBatchSize() {
        return threadOplogBatchSize;
    }

    public TableGroupConfig setThreadOplogBatchSize(int threadOplogBatchSize) {
        this.threadOplogBatchSize = threadOplogBatchSize;
        return this;
    }

    public int getServerPushRowThreshold() {
        return serverPushRowThreshold;
    }

    public TableGroupConfig setServerPushRowThreshold(int serverPushRowThreshold) {
        this.serverPushRowThreshold = serverPushRowThreshold;
        return this;
    }

    public long getBgIdleMilli() {
        return bgIdleMilli;
    }

    public TableGroupConfig setBgIdleMilli(long bgIdleMilli) {
        this.bgIdleMilli = bgIdleMilli;
        return this;
    }

    public long getServerIdleMilli() {
        return serverIdleMilli;
    }

    public TableGroupConfig setServerIdleMilli(long serverIdleMilli) {
        this.serverIdleMilli = serverIdleMilli;
        return this;
    }
}
