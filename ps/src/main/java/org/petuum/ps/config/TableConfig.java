package org.petuum.ps.config;

import org.petuum.ps.row.RowTypeId;
import org.petuum.ps.row.RowUpdateTypeId;

/**
 * Created by aqiao on 3/5/15.
 */
public class TableConfig {

    private Config rowConfig;
    private Config rowUpdateConfig;

    private int staleness;
    private int rowType;
    private int rowUpdateType;
    private int processCacheCapacity;
    private int threadCacheCapacity;
    private boolean noOplogReplay;

    public TableConfig(TableConfig other) {
        this.rowConfig = other.rowConfig.getCopy();
        this.rowUpdateConfig = other.rowUpdateConfig.getCopy();
        this.staleness = other.staleness;
        this.rowType = other.rowType;
        this.rowUpdateType = other.rowUpdateType;
        this.processCacheCapacity = other.processCacheCapacity;
        this.threadCacheCapacity = other.threadCacheCapacity;
        this.noOplogReplay = other.noOplogReplay;
    }

    public TableConfig() {
        this.rowConfig = new Config();
        this.rowUpdateConfig = new Config();
        this.staleness = 0;
        this.rowType = RowTypeId.INVALID;
        this.rowUpdateType = RowUpdateTypeId.INVALID;
        this.processCacheCapacity = 1;
        this.threadCacheCapacity = 1;
        this.noOplogReplay = false;
    }

    public Config getRowConfig() {
        return this.rowConfig;
    }

    public TableConfig setRowConfig(Config rowConfig) {
        this.rowConfig = rowConfig;
        return this;
    }

    public Config getRowUpdateConfig() {
        return this.rowUpdateConfig;
    }

    public TableConfig setRowUpdateConfig(Config rowUpdateConfig) {
        this.rowUpdateConfig = rowUpdateConfig;
        return this;
    }

    public int getStaleness() {
        return staleness;
    }

    public TableConfig setStaleness(int staleness) {
        this.staleness = staleness;
        return this;
    }

    public int getRowType() {
        return rowType;
    }

    public TableConfig setRowType(int rowType) {
        this.rowType = rowType;
        return this;
    }

    public int getRowUpdateType() {
        return rowUpdateType;
    }

    public TableConfig setRowUpdateType(int rowUpdateType) {
        this.rowUpdateType = rowUpdateType;
        return this;
    }


    public int getProcessCacheCapacity() {
        return processCacheCapacity;
    }

    public TableConfig setProcessCacheCapacity(int processCacheCapacity) {
        this.processCacheCapacity = processCacheCapacity;
        return this;
    }

    public boolean isNoOplogReplay() {
        return noOplogReplay;
    }

    public TableConfig setNoOplogReplay(boolean noOplogReplay) {
        this.noOplogReplay = noOplogReplay;
        return this;
    }

    public int getThreadCacheCapacity() {
        return threadCacheCapacity;
    }

    public TableConfig setThreadCacheCapacity(int threadCacheCapacity) {
        this.threadCacheCapacity = threadCacheCapacity;
        return this;
    }
}
