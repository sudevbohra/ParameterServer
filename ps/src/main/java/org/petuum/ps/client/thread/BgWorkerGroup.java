package org.petuum.ps.client.thread;

import com.google.common.base.Preconditions;
import gnu.trove.map.TIntObjectMap;
import org.petuum.ps.client.ClientTable;
import org.petuum.ps.common.msg.MsgType;
import org.petuum.ps.common.network.Msg;
import org.petuum.ps.common.util.IntBox;
import org.petuum.ps.config.TableConfig;

import java.util.ArrayList;
import java.util.ListIterator;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class BgWorkerGroup {
    protected TIntObjectMap<ClientTable> tables;
    protected ArrayList<AbstractBgWorker> bgWorkerVec;
    protected int bgWorkerIdSt;

    CyclicBarrier initBarrier;
    CyclicBarrier createTableBarrier;

    public BgWorkerGroup(TIntObjectMap<ClientTable> tables) {
        this.tables = tables;
        this.bgWorkerVec = new ArrayList<>();
        for (int i = 0; i < GlobalContext.getNumCommChannelsPerClient(); i++) {
            this.bgWorkerVec.add(null);
        }
        this.bgWorkerIdSt = GlobalContext.getHeadBgId(GlobalContext
                .getClientId());

        initBarrier = new CyclicBarrier(
                GlobalContext.getNumCommChannelsPerClient() + 1);
        createTableBarrier = new CyclicBarrier(
                GlobalContext.getNumCommChannelsPerClient() + 1);
    }

    public void close() {
        ListIterator<AbstractBgWorker> it = bgWorkerVec.listIterator();
        while (it.hasNext()) {
            AbstractBgWorker worker = it.next();
            if (worker != null) {
                it.set(null);
            }
        }
    }

    public void createBgWorkers() {
        int idx = 0;
        ListIterator<AbstractBgWorker> it = bgWorkerVec.listIterator();
        while (it.hasNext()) {
            it.next();
            it.set(new SSPBgWorker(bgWorkerIdSt + idx, idx, tables,
                    initBarrier, createTableBarrier));
            ++idx;
        }
    }

    public void start() {
        createBgWorkers();
        for (AbstractBgWorker bgWorker : bgWorkerVec) {
            bgWorker.start();
        }
        try {
            initBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void shutdown() {
        for (AbstractBgWorker bgWorker : bgWorkerVec) {
            bgWorker.shutdown();
        }
    }

    public void appThreadRegister() {
        for (AbstractBgWorker bgWorker : bgWorkerVec) {
            bgWorker.appThreadRegister();
        }
    }

    public void appThreadDeregister() {
        for (AbstractBgWorker bgWorker : bgWorkerVec) {
            bgWorker.appThreadDeregister();
        }
    }

    public boolean createTable(int tableId, TableConfig tableConfig) {
        return bgWorkerVec.get(0).createTable(tableId, tableConfig);
    }

    public void waitCreateTable() {
        try {
            createTableBarrier.await();
        } catch (Exception e) {
            System.err.println("FATAL ERROR IN waitCreateTable!!");
        }
    }

    public boolean requestRow(int tableId, int rowId, int clock) {
        int bgIdx = GlobalContext.getPartitionCommChannelIndex(rowId);
        return bgWorkerVec.get(bgIdx).requestRow(tableId, rowId, clock);
    }

    public void requestRowAsync(int tableId, int rowId, int clock,
                                boolean forced) {
        int bgIdx = GlobalContext.getPartitionCommChannelIndex(rowId);
        bgWorkerVec.get(bgIdx).requestRowAsync(tableId, rowId, clock, forced);
    }

    public void getAsyncRowRequestReply() {
        Msg msg = GlobalContext.commBus.recv();
        assert(msg.getMsgType() == MsgType.ROW_REQUEST_REPLY);
    }

    public void signalHandleAppendOnlyBuffer(int tableId, int channelIdx) {
        // TODO: Implement this!
        System.err
                .println("UNIMPLEMENTED METHOD CALL signalHandleAppendOnlyBuffer");
        Preconditions.checkArgument(false);
        // bgWorkerVec.get(channelIdx).signalHandleAppendOnlyBuffer(tableId);
    }

    public void clockAllTables() {
        for (AbstractBgWorker bgWorker : bgWorkerVec) {
            bgWorker.clockAllTables();
        }
    }

    public void sendOpLogsAllTables() {
        for (AbstractBgWorker bgWorker : bgWorkerVec) {
            bgWorker.sendOpLogsAllTables();
        }
    }

    public int getSystemClock() {
        System.err.println("Not supported function");
        return 0;
    }

    public void waitSystemClock(int myClock) {
        System.err.println("Not supported function");
    }
}
