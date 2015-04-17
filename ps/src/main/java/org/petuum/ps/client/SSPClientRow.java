package org.petuum.ps.client;

import org.petuum.ps.common.util.IntBox;
import org.petuum.ps.row.Row;

public class SSPClientRow extends ClientRow {

    private final IntBox clock;

    public SSPClientRow(int clock, Row rowData, boolean useRefCount) {
        super(clock, rowData, useRefCount);
        this.clock = new IntBox(clock);
    }

    @Override
    public int getClock() {
        synchronized (this.clock) {
            return this.clock.intValue;
        }
    }

    @Override
    public void setClock(int clock) {
        synchronized (this.clock) {
            this.clock.intValue = clock;
        }
    }
}
