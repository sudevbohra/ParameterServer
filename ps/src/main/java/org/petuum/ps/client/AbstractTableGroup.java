package org.petuum.ps.client;

import org.petuum.ps.config.TableConfig;

public abstract class AbstractTableGroup {

    public AbstractTableGroup() {

    }

    public void close() {
    }

    public abstract boolean createTable(int table_id, TableConfig table_config);

    public abstract void createTableDone();

    public abstract void waitThreadRegister();

    public abstract AbstractClientTable getTableOrDie(int table_id);

    public abstract int registerThread();

    public abstract void deregisterThread();

    public abstract void clock();

    public abstract void globalBarrier();

}
