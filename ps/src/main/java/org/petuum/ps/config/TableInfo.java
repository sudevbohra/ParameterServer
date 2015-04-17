package org.petuum.ps.config;

/**
 * Created by aqiao on 3/5/15.
 */
public class TableInfo {
    public int tableStaleness;
    public int rowType;
    public int rowUpdateType;

    public TableInfo() {
        tableStaleness = 0;
        rowType = 0;
        rowUpdateType = 1;
    }

    public TableInfo(TableInfo info) {
        this.tableStaleness = info.tableStaleness;
        this.rowType = info.rowType;
        this.rowUpdateType = info.rowUpdateType;
    }
}
