package org.petuum.ps.client;

import java.util.Map;

import gnu.trove.map.TIntObjectMap;

import org.petuum.ps.client.consistency.AbstractConsistencyController;
import org.petuum.ps.client.consistency.SSPConsistencyController;
import org.petuum.ps.client.consistency.SSPPushConsistencyController;
import org.petuum.ps.client.oplog.Oplog;
import org.petuum.ps.client.oplog.TableOpLogIndex;
import org.petuum.ps.client.storage.ProcessStorage;
import org.petuum.ps.client.storage.SparseProcessStorage;
import org.petuum.ps.client.thread.GlobalContext;
import org.petuum.ps.common.FactoryRegistry;
import org.petuum.ps.config.ConsistencyModel;
import org.petuum.ps.config.TableConfig;
import org.petuum.ps.row.Row;
import org.petuum.ps.row.RowFactory;
import org.petuum.ps.row.RowUpdate;
import org.petuum.ps.row.RowUpdateFactory;

public class ClientTable extends AbstractClientTable {

    private int tableId;
    private Oplog oplog;
    private ProcessStorage processStorage;
    private AbstractConsistencyController consistencyController;
    private ThreadLocal<ThreadTable> threadCache;

    private TableOpLogIndex oplogIndex;
    private TableConfig tableConfig;

    private RowFactory rowFactory;
    private RowUpdateFactory rowUpdateFactory;

    public ClientTable(int tableId, TableConfig tableConfig) {
        this.tableId = tableId;
        this.tableConfig = tableConfig;
        // FIXME: change this
        this.oplogIndex = new TableOpLogIndex(1000000);
        this.threadCache = new ThreadLocal<>();

        this.rowFactory = FactoryRegistry.getRowFactory(tableConfig
                .getRowType());
        this.rowUpdateFactory = FactoryRegistry.getRowUpdateFactory(tableConfig
                .getRowUpdateType());

        this.processStorage = new SparseProcessStorage();
        this.oplog = new Oplog(this.rowUpdateFactory,
                this.tableConfig.getRowUpdateConfig());

        switch (GlobalContext.getConsistencyModel()) {
        case ConsistencyModel.SSP:
            this.consistencyController = new SSPConsistencyController(
                    tableConfig, tableId, this.processStorage, this.oplog,
                    this.threadCache, this.oplogIndex);
            break;
        case ConsistencyModel.SSPPush:
            this.consistencyController = new SSPPushConsistencyController(
                    tableConfig, tableId, this.processStorage, this.oplog,
                    this.threadCache, this.oplogIndex);
            break;
        case ConsistencyModel.SSPAggr:
            // FIXME: implement this
            break;
        default:
            System.out.println("Unknown consistency model");
        }
    }

    public RowUpdateFactory getRowUpdateFactory() {
        return this.rowUpdateFactory;
    }

    public RowFactory getRowFactory() {
        return this.rowFactory;
    }

    @Override
    public void close() {
        this.consistencyController.close();
    }

    public void registerThread() {
        if (this.threadCache.get() == null) {
            this.threadCache.set(new ThreadTable(this.tableConfig));
        }
    }

    public void deregisterThread() {
        this.threadCache.set(null);
    }

    public void getAsyncForced(int rowId) {
        this.consistencyController.getAsyncForced(rowId);
    }

    public void getAsync(int rowId) {
        consistencyController.getAsync(rowId);
    }

    public void waitPendingAsyncGet() {
        consistencyController.waitPendingAsnycGet();
    }

    public Row threadGet(int rowId) {
        return consistencyController.threadGet(rowId);
    }

    public void threadBatchInc(int rowId, RowUpdate rowUpdate) {
        consistencyController.threadBatchInc(rowId, rowUpdate);
    }

    public void batchInc(int rowId, RowUpdate rowUpdate) {
        consistencyController.batchInc(rowId, rowUpdate);
    }

    public void flushThreadCache() {
        consistencyController.flushThreadCache();
    }

    public ClientRow get(int rowId) {
        return consistencyController.get(rowId);
    }

    public void clock() {
        consistencyController.clock();
    }

    public Map<Integer, Boolean> getAndResetOpLogIndex(int partitionNum) {
        return oplogIndex.resetPartition(partitionNum);
    }

    public int getNumRowOpLogs(int partitionNum) {
        return oplogIndex.getNumRowOpLogs(partitionNum);
    }

    public int getRowType() {
        return this.tableConfig.getRowType();
    }

    public ProcessStorage getProcessStorage() {
        return this.processStorage;
    }

    public boolean isNoOplogReplay() {
        return this.tableConfig.isNoOplogReplay();
    }

    public Oplog getOplog() {
        return this.oplog;
    }

    public Row createRow() {
        return this.rowFactory.createRow(this.tableConfig.getRowConfig());
    }

}
