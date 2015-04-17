package org.petuum.ps.client;

import org.petuum.ps.row.Row;

import java.util.concurrent.atomic.AtomicInteger;

public class ClientRow {

    AtomicInteger num_refs_;
    private Row row_data_ptr_;
    private Func IncRef_;
    private Func DecRef_;

    public ClientRow(int clock, Row row_data, boolean use_ref_count) {
        num_refs_ = new AtomicInteger();
        row_data_ptr_ = row_data;
        if (use_ref_count) {
            IncRef_ = new DoIncRef();
            DecRef_ = new DoDecRef();
        } else {
            IncRef_ = new DoNothing();
            DecRef_ = new DoNothing();
        }
    }

    public void setClock(int clock) {
    }

    public int getClock() {
        return -1;
    }

    public Row getRowDataPtr() {
        return row_data_ptr_;
    }

    public boolean HasZeroRef() {
        return (num_refs_.get() == 0);
    }

    public void incRef() {
        IncRef_.call();
    }

    public void decRef() {
        DecRef_.call();
    }

    public int get_num_refs() {
        return num_refs_.get();
    }

    private interface Func {
        public void call();
    }

    private class DoIncRef implements Func {
        @Override
        public void call() {
            num_refs_.incrementAndGet();
        }
    }

    private class DoDecRef implements Func {
        @Override
        public void call() {
            num_refs_.decrementAndGet();
        }
    }

    private class DoNothing implements Func {
        @Override
        public void call() {
        }
    }
}
