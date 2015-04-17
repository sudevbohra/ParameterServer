package org.petuum.ps.client.thread;

import gnu.trove.map.TIntObjectMap;
import org.petuum.ps.client.ClientTable;
import org.petuum.ps.config.ConsistencyModel;
import org.petuum.ps.config.TableConfig;

public class BgWorkers {
    private static BgWorkerGroup bgWorkerGroup;

    public static void start(TIntObjectMap<ClientTable> tables) {
        int consistencyModel = GlobalContext.getConsistencyModel();
        switch (consistencyModel) {
            case ConsistencyModel.SSP:
                bgWorkerGroup = new BgWorkerGroup(tables);
                break;
            case ConsistencyModel.SSPPush:
            case ConsistencyModel.SSPAggr:
                // TODO: implement these
                break;
            default:
                System.err.println("Unknown consistency model");
        }
        bgWorkerGroup.start();
    }

    public static void shutdown() {
        bgWorkerGroup.shutdown();
        bgWorkerGroup.close();
    }

    public static void appThreadRegister() {
        bgWorkerGroup.appThreadRegister();
    }

    public static void appThreadDeregister() {
        bgWorkerGroup.appThreadDeregister();
    }

    public static boolean createTable(int tableId, TableConfig tableConfig) {
        return bgWorkerGroup.createTable(tableId, tableConfig);
    }

    public static void waitCreateTable() {
        bgWorkerGroup.waitCreateTable();
    }

    public static boolean requestRow(int tableId, int rowId, int clock) {
        return bgWorkerGroup.requestRow(tableId, rowId, clock);
    }

    public static void requestRowAsync(int tableId, int rowId, int clock,
                                       boolean forced) {
        bgWorkerGroup.requestRowAsync(tableId, rowId, clock, forced);
    }

    public static void getAsyncRowRequestReply() {
        bgWorkerGroup.getAsyncRowRequestReply();
    }

    public static void signalHandleAppendOnlyBuffer(int tableId, int channelIdx) {
        bgWorkerGroup.signalHandleAppendOnlyBuffer(tableId, channelIdx);
    }

    public static void clockAllTables() {
        bgWorkerGroup.clockAllTables();
    }

    public static void sendOpLogsAllTables() {
        bgWorkerGroup.sendOpLogsAllTables();
    }

    public static int getSystemClock() {
        return bgWorkerGroup.getSystemClock();
    }

    public static void waitSystemClock(int myClock) {
        bgWorkerGroup.waitSystemClock(myClock);
    }
}
