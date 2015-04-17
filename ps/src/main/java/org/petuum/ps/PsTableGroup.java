package org.petuum.ps;

import org.petuum.ps.client.AbstractClientTable;
import org.petuum.ps.client.AbstractTableGroup;
import org.petuum.ps.client.TableGroup;
import org.petuum.ps.common.util.IntBox;
import org.petuum.ps.config.Config;
import org.petuum.ps.config.ConfigKey;
import org.petuum.ps.config.TableConfig;
import org.petuum.ps.config.TableGroupConfig;
import org.petuum.ps.row.RowTypeId;
import org.petuum.ps.row.RowUpdateTypeId;
import org.petuum.ps.table.DoubleTable;
import org.petuum.ps.table.FloatTable;
import org.petuum.ps.table.IntTable;
import org.petuum.ps.table.LongTable;

public class PsTableGroup {

    private static AbstractTableGroup abstractTableGroup;

    // Can be called only once per process. Must be called after registerRow()
    // to
    // be a barrier for createTable(), and "happen before" any other call to
    // TableGroup. The thread that calls init() is refered to as the init
    // thread. If the init thread needs to access table API (e.g., init thread
    // itself being a worker thread), it should set tableAccess to true. init
    // thread is responsible for calling registerRow(), createTable() and
    // shutdown(). Calling those functions from other threads is not allowed.
    // init thread does not need to deregisterThread() nor registerThread().
    public static int init(TableGroupConfig tableGroupConfig,
                           boolean tableAccess) {
        IntBox initThreadId = new IntBox(0);

        abstractTableGroup = new TableGroup(tableGroupConfig, tableAccess,
                initThreadId);

        return initThreadId.intValue;
    }

    // Default tableAccess to false.
    public static int init(TableGroupConfig tableGroupConfig) {
        return init(tableGroupConfig, false);
    }

    public static void shutdown() {
        abstractTableGroup.close();
        abstractTableGroup = null;
    }

    public static boolean createTable(int tableId, TableConfig tableConfig) {
        return abstractTableGroup.createTable(tableId, new TableConfig(tableConfig));
    }

    public static boolean createDenseDoubleTable(int tableId, int rowCapacity, TableConfig tableConfig) {
        tableConfig = new TableConfig(tableConfig);
        Config rowConfig = new Config();
        rowConfig.putInt(ConfigKey.DENSE_ROW_CAPACITY, rowCapacity);
        Config rowUpdateConfig = new Config();
        rowUpdateConfig.putInt(ConfigKey.DENSE_ROW_UPDATE_CAPACITY, rowCapacity);
        tableConfig.setRowType(RowTypeId.DENSE_DOUBLE);
        tableConfig.setRowConfig(rowConfig);
        tableConfig.setRowUpdateType(RowUpdateTypeId.DENSE_DOUBLE);
        tableConfig.setRowUpdateConfig(rowUpdateConfig);
        return createTable(tableId, tableConfig);
    }

    public static boolean createSparseDoubleTable(int tableId, TableConfig tableConfig) {
        tableConfig = new TableConfig(tableConfig);
        Config rowConfig = new Config();
        Config rowUpdateConfig = new Config();
        tableConfig.setRowType(RowTypeId.SPARSE_DOUBLE);
        tableConfig.setRowConfig(rowConfig);
        tableConfig.setRowUpdateType(RowUpdateTypeId.SPARSE_DOUBLE);
        tableConfig.setRowUpdateConfig(rowUpdateConfig);
        return createTable(tableId, tableConfig);
    }

    public static boolean createDenseIntTable(int tableId, int rowCapacity, TableConfig tableConfig) {
        tableConfig = new TableConfig(tableConfig);
        Config rowConfig = new Config();
        rowConfig.putInt(ConfigKey.DENSE_ROW_CAPACITY, rowCapacity);
        Config rowUpdateConfig = new Config();
        rowUpdateConfig.putInt(ConfigKey.DENSE_ROW_UPDATE_CAPACITY, rowCapacity);
        tableConfig.setRowType(RowTypeId.DENSE_INT);
        tableConfig.setRowConfig(rowConfig);
        tableConfig.setRowUpdateType(RowUpdateTypeId.DENSE_INT);
        tableConfig.setRowUpdateConfig(rowUpdateConfig);
        return createTable(tableId, tableConfig);
    }

    public static boolean createSparseIntTable(int tableId, TableConfig tableConfig) {
        tableConfig = new TableConfig(tableConfig);
        Config rowConfig = new Config();
        Config rowUpdateConfig = new Config();
        tableConfig.setRowType(RowTypeId.SPARSE_INT);
        tableConfig.setRowConfig(rowConfig);
        tableConfig.setRowUpdateType(RowUpdateTypeId.SPARSE_INT);
        tableConfig.setRowUpdateConfig(rowUpdateConfig);
        return createTable(tableId, tableConfig);
    }

    public static boolean createDenseFloatTable(int tableId, int rowCapacity, TableConfig tableConfig) {
        tableConfig = new TableConfig(tableConfig);
        Config rowConfig = new Config();
        rowConfig.putFloat(ConfigKey.DENSE_ROW_CAPACITY, rowCapacity);
        Config rowUpdateConfig = new Config();
        rowUpdateConfig.putFloat(ConfigKey.DENSE_ROW_UPDATE_CAPACITY, rowCapacity);
        tableConfig.setRowType(RowTypeId.DENSE_FLOAT);
        tableConfig.setRowConfig(rowConfig);
        tableConfig.setRowUpdateType(RowUpdateTypeId.DENSE_FLOAT);
        tableConfig.setRowUpdateConfig(rowUpdateConfig);
        return createTable(tableId, tableConfig);
    }

    public static boolean createSparseFloatTable(int tableId, TableConfig tableConfig) {
        tableConfig = new TableConfig(tableConfig);
        Config rowConfig = new Config();
        Config rowUpdateConfig = new Config();
        tableConfig.setRowType(RowTypeId.SPARSE_FLOAT);
        tableConfig.setRowConfig(rowConfig);
        tableConfig.setRowUpdateType(RowUpdateTypeId.SPARSE_FLOAT);
        tableConfig.setRowUpdateConfig(rowUpdateConfig);
        return createTable(tableId, tableConfig);
    }

    public static boolean createDenseLongTable(int tableId, int rowCapacity, TableConfig tableConfig) {
        tableConfig = new TableConfig(tableConfig);
        Config rowConfig = new Config();
        rowConfig.putLong(ConfigKey.DENSE_ROW_CAPACITY, rowCapacity);
        Config rowUpdateConfig = new Config();
        rowUpdateConfig.putLong(ConfigKey.DENSE_ROW_UPDATE_CAPACITY, rowCapacity);
        tableConfig.setRowType(RowTypeId.DENSE_LONG);
        tableConfig.setRowConfig(rowConfig);
        tableConfig.setRowUpdateType(RowUpdateTypeId.DENSE_LONG);
        tableConfig.setRowUpdateConfig(rowUpdateConfig);
        return createTable(tableId, tableConfig);
    }

    public static boolean createSparseLongTable(int tableId, TableConfig tableConfig) {
        tableConfig = new TableConfig(tableConfig);
        Config rowConfig = new Config();
        Config rowUpdateConfig = new Config();
        tableConfig.setRowType(RowTypeId.SPARSE_LONG);
        tableConfig.setRowConfig(rowConfig);
        tableConfig.setRowUpdateType(RowUpdateTypeId.SPARSE_LONG);
        tableConfig.setRowUpdateConfig(rowUpdateConfig);
        return createTable(tableId, tableConfig);
    }

    // Must be called by init thread after creating all tables and before any
    // other thread calls registerThread().
    public static void createTableDone() {
        abstractTableGroup.createTableDone();
    }

    // Called by init thread only before it access any table API.
    // Must be called after createTableDone().
    // If init thread does not access table API, it makes no difference calling
    // this function.
    public static void waitThreadRegister() {
        abstractTableGroup.waitThreadRegister();
    }

    // getTableOrDie is thread-safe with respect to other calls to
    // getTableOrDie() Getter, terminate if table is not found.
    public static DoubleTable getDoubleTableOrDie(int tableId) {
        AbstractClientTable abstractTable = abstractTableGroup.getTableOrDie(tableId);
        return new DoubleTable(abstractTable);
    }

    public static IntTable getIntTableOrDie(int tableId) {
        AbstractClientTable abstractTable = abstractTableGroup.getTableOrDie(tableId);
        return new IntTable(abstractTable);
    }

    public static FloatTable getFloatTableOrDie(int tableId) {
        AbstractClientTable abstractTable = abstractTableGroup.getTableOrDie(tableId);
        return new FloatTable(abstractTable);
    }

    public static LongTable getLongTableOrDie(int tableId) {
        AbstractClientTable abstractTable = abstractTableGroup.getTableOrDie(tableId);
        return new LongTable(abstractTable);
    }

    // A app threads except init thread should register itself before accessing
    // any Table API. In SSP mode, if a thread invokes registerThread with
    // true, its clock will be kept track of, so it should call clock()
    // properly.
    public static int registerThread() {
        return abstractTableGroup.registerThread();
    }

    // A registered thread must deregister itself.
    public static void deregisterThread() {
        abstractTableGroup.deregisterThread();
    }

    // Advance clock for the application thread.
    //
    // We only use one vector clock per process, each clock for a
    // registered app thread. The vector clock is not associated with individual
    // tables.
    public static void clock() {
        abstractTableGroup.clock();
    }

    // Called by application threads that access table API
    // (referred to as table threads).
    // Threads that calls globalBarrier must be at the same clock.
    // 1) A table thread may not go beyond the barrier until all table threads
    // have reached the barrier;
    // 2) Table threads that move beyond the barrier are guaranteed to see
    // the updates that other table threads apply to the table.
    public static void globalBarrier() {
        abstractTableGroup.globalBarrier();
    }
}
