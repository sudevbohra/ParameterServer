package org.petuum.app.hello_ssp;

import org.petuum.ps.config.*;
import org.petuum.ps.PsTableGroup;
import org.petuum.ps.row.double_.DenseDoubleRow;
import org.petuum.ps.row.double_.DenseDoubleRowUpdate;
import org.petuum.ps.row.double_.DoubleRow;
import org.petuum.ps.row.double_.DoubleRowUpdate;
import org.petuum.ps.table.DoubleTable;
import org.petuum.ps.common.util.Timer;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

// A demo to show SSP guarantee. The only table has only 1 row. That row of
// numWorkers columns, where numWorkers is the number of application threads
// across nodes. Each column of the row represent the clock number of
// a thread. This app shows that the clock from other workers is bounded by SSP.
public class HelloSSP {
    // Just 1 sspTable
    private static final int kNumTables = 1;

    // Each table in PS has an id.
    private static final int kSumTableId = 0;

    // Command line arguments.
    private static class CmdArgs {
        // PS parameters:
        @Option(name = "-clientId", required = true,
                usage = "Client Id is the rank of this client. Default = 0")
        public int clientId = 0;

        @Option(name = "-hostFile", required = true,
                usage = "Path to host file.")
        public String hostFile = "";

        @Option(name = "-numWorkerThreads", required = false,
                usage = "Client Id is the rank of this client. " +
                "Default = 1")
        public int numWorkerThreads = 1;

        @Option(name = "-staleness", required = false,
                usage = "Staleness of parameter tables. Default = 0")
        public int staleness = 0;

        @Option(name = "-cacheSize", required = false,
                usage = "Number of table rows to cache locally. "
                + "Default = 1000000")
        public int cacheSize = 1000000;

        // HelloSSP parameters:
        @Option(name = "-numIterations", required = false,
                usage = "Number of iterations. Default = 10")
        public int numIterations = 10;
    }

    // Worker thread logic.
    private static class HelloWorker implements Runnable {
        private int workerRank;
        private int numIterations;
        private int numWorkers;
        private int staleness;

        public static class Config {
            public int workerRank = -1;
            public int numWorkers = -1;
            public int numIterations = 1;
            public int staleness = 0;
        }

        public HelloWorker(Config config) {
            assert config.workerRank != -1;
            assert config.numWorkers != -1;
            this.workerRank = config.workerRank;
            this.numWorkers = config.numWorkers;
            this.numIterations = config.numIterations;
            this.staleness = config.staleness;
        }

        public void run() {
            // Let PS know this thread wants to interact with it.
            PsTableGroup.registerThread();
            int rowId = 0;

            // Get table by ID.
            DoubleTable sspTable =
                PsTableGroup.getDoubleTableOrDie(kSumTableId);

            for (int iter = 0; iter < numIterations; iter++) {
                // Check that we are reading from other workers with SSP
                // bound.
                DoubleRow r = sspTable.get(rowId);
                // This is the clock number we should get from other users.
                double lowerBound = iter - staleness;
                for (int col = 0; col < numWorkers; col++) {
                    double val = r.get(col);
                    assert val >= lowerBound :
                        "lowerBound: " + lowerBound + " but got " + val;
                }
                if (workerRank == 0) {
                    System.out.println("Worker " + workerRank +
                            ": I'm reading pretty fresh stuff!");
                }

                // We can do batch read like this. This way we don't
                // acquire lock every r.get(col) as above.
                DoubleRow rCache = new DenseDoubleRow(numWorkers);
                rCache.reset(r);
                for (int col = 0; col < numWorkers; ++col) {
                    double val = rCache.getUnlocked(col);
                    assert val >= lowerBound;
                }

                // We can also batchInc another row (which we don't care).
                int batchIncRow = 1;
                DoubleRowUpdate batchIncUpdate =
                    new DenseDoubleRowUpdate(numWorkers);
                // Increment row 0 by [0, 2, 4, ...,].
                for (int col = 0; col < numWorkers; ++col) {
                    batchIncUpdate.setUpdate(col, iter);
                }
                sspTable.batchInc(batchIncRow, batchIncUpdate);

                // Increment the entry in sspTable.
                sspTable.inc(rowId, workerRank, 1);

                // Finally, finish this clock.
                PsTableGroup.clock();
            }

            // globalBarrier makes subsequent read all fresh.
            PsTableGroup.globalBarrier();

            // Read updates. After globalBarrier all updates should
            // be visible.
            DoubleRow r = sspTable.get(rowId);
            for (int col = 0; col < numWorkers; col++) {
                double val = r.get(col);
                double expectedVal = numIterations;
                assert val == expectedVal :
                    "expected: " + expectedVal + " but got " + val;
            }
            if (workerRank == 0) {
                System.out.println("All entries are fresh after " +
                        "globalBarrier()!");
            }

            // Tell PS I'm done for ordered shutdown.
            PsTableGroup.deregisterThread();
        }
    }

    public static void main(String[] args) {
        final CmdArgs cmd = new CmdArgs();
        final CmdLineParser parser = new CmdLineParser(cmd);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        }

        // Configuring PS.
        TableGroupConfig tgConfig = new TableGroupConfig(cmd.hostFile);
        tgConfig.setNumTables(kNumTables)
            // +1 for main thread.
            .setNumLocalAppThreads(cmd.numWorkerThreads + 1)
            .setClientId(cmd.clientId);
        PsTableGroup.init(tgConfig);

        // Configure sspTable.
        TableConfig table_config = new TableConfig();
        table_config.setStaleness(cmd.staleness)
            .setProcessCacheCapacity(cmd.cacheSize);
        // Each column will track the clock of a worker.
        int numClients = tgConfig.getNumTotalClients();
        int numWorkers = cmd.numWorkerThreads * numClients;
        PsTableGroup.createDenseDoubleTable(kSumTableId,
                numWorkers, table_config);

        // Finalize table creation.
        PsTableGroup.createTableDone();

        Timer timer = new Timer();
        if (cmd.clientId == 0) {
            System.out.println("Starting " + numClients +
                    " nodes, each with " + cmd.numWorkerThreads + " threads.");
        }
        // Creating worker threads.
        Thread[] workers = new Thread[cmd.numWorkerThreads];
        for (int i = 0; i < cmd.numWorkerThreads; i++) {
            HelloWorker.Config helloConfig = new HelloWorker.Config();
            helloConfig.workerRank = cmd.clientId * cmd.numWorkerThreads + i;
            helloConfig.numWorkers = numWorkers;
            helloConfig.numIterations = cmd.numIterations;
            workers[i] = new Thread(new HelloWorker(helloConfig));
            workers[i].start();
        }

        // Waiting for them to finish.
        for (Thread t : workers) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        PsTableGroup.shutdown();
        if (cmd.clientId == 0) {
            System.out.println("HelloSSP finished in " + timer.elapsed() +
                    "s.");
        }
    }
}
