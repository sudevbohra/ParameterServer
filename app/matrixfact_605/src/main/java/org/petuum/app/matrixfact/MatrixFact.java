package org.petuum.app.matrixfact;

import org.petuum.app.matrixfact.Rating;
import org.petuum.app.matrixfact.DataLoader;
import org.petuum.app.matrixfact.MatrixFactWorker;
import org.petuum.ps.PsTableGroup;
import org.petuum.ps.config.*;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.petuum.ps.common.util.Timer;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import static org.kohsuke.args4j.ExampleMode.ALL;

public class MatrixFact {
    private static final Logger logger =
        LoggerFactory.getLogger(MatrixFact.class);

    private static final int LTableId = 0;
    private static final int RTableId = 1;

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

        // MatrixFact parameters:
        @Option(name = "-dataFile", required =true,
                usage = "Path to data file.")
        public String dataFile = "";

        @Option(name = "-numEpochs", required = false,
                usage = "Number of passes over data. Default = 10")
        public int numEpochs = 10;

        @Option(name = "-K", required = false,
                usage = "Rank of factor matrices. Default = 20")
        public int K = 20;

        @Option(name = "-lambda", required = false,
                usage = "Regularization parameter lambda. Default = 0.1")
        public double lambda = 0.1f;

        @Option(name = "-learningRateDecay", required = false,
                usage = "Learning rate parameter. Default = 1")
        public double learningRateDecay = 1f;

        @Option(name = "-learningRateEta0", required = false,
                usage = "Learning rate parameter. Default = 0.1")
        public double learningRateEta0 = 0.1f;

        @Option(name = "-numMiniBatchesPerEpoch", required = false,
                usage = "Equals to number of clock() calls per data sweep. "
                + "Default = 1")
        public int numMiniBatchesPerEpoch = 1;

        @Option(name = "-outputPrefix", required =false,
                usage = "Output to outputPrefix.L, outputPrefix.W.")
        public String outputPrefix = "";
    }

    public static void main(String[] args) {
        Timer totalTimer = new Timer();
        final CmdArgs cmd = new CmdArgs();
        final CmdLineParser parser = new CmdLineParser(cmd);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            logger.error(e.getMessage());
            parser.printUsage(System.err);
            return;
        }

        // Configuring PS.
        TableGroupConfig tgConfig = new TableGroupConfig(cmd.hostFile);
        int numTables = 2 + 1;    // L, R, +1 for loss table.
        tgConfig.setNumTables(numTables)
            // +1 for main thread.
            .setNumLocalAppThreads(cmd.numWorkerThreads + 1)
            .setClientId(cmd.clientId);
        PsTableGroup.init(tgConfig);

        // Configure L, R tables.
        TableConfig tableConfig = new TableConfig();
        tableConfig.setStaleness(cmd.staleness)
            .setProcessCacheCapacity(cmd.cacheSize);
        // K+1 columns. For LTable, row i column K is nnz ratings in row i of
        // data matrix D.
        PsTableGroup.createDenseDoubleTable(LTableId, cmd.K + 1, tableConfig);
        PsTableGroup.createDenseDoubleTable(RTableId, cmd.K + 1, tableConfig);

        // Configure loss table
        LossRecorder.createLossTable();

        // Finalize table creation.
        PsTableGroup.createTableDone();

        Timer loadTimer = new Timer();
        ArrayList<Rating> ratings = new ArrayList<Rating>();
        int[] dim = DataLoader.ReadData(cmd.dataFile, ratings);
        logger.info("Client " + cmd.clientId + " read data (" + dim[0] +
                " rows, " + dim[1] + " cols, " + ratings.size() +
                " ratings) in " + loadTimer.elapsed() + "s");

        Timer trainTimer = new Timer();
        int numClients = tgConfig.getNumTotalClients();
        int numThreads = cmd.numWorkerThreads;
        if (cmd.clientId == 0) {
            logger.info("Starting " + numClients +
                    " nodes, each with " + numThreads + " threads.");
        }
        // Creating worker threads.
        Thread[] workers = new Thread[cmd.numWorkerThreads];
        for (int i = 0; i < cmd.numWorkerThreads; i++) {
            // Set up config.
            MatrixFactWorker.Config mfConfig = new MatrixFactWorker.Config();
            mfConfig.numWorkers = numClients * numThreads;
            mfConfig.workerRank = numThreads * cmd.clientId + i;
            mfConfig.numThreads = numThreads;
            mfConfig.numEpochs = cmd.numEpochs;
            mfConfig.ratings = ratings;
            mfConfig.LTableId = LTableId;
            mfConfig.RTableId = RTableId;
            mfConfig.K = cmd.K;
            mfConfig.numRows = dim[0];
            mfConfig.numCols = dim[1];
            mfConfig.lambda = cmd.lambda;
            mfConfig.learningRateDecay = cmd.learningRateDecay;
            mfConfig.learningRateEta0 = cmd.learningRateEta0;
            mfConfig.numMiniBatchesPerEpoch = cmd.numMiniBatchesPerEpoch;
            mfConfig.staleness = cmd.staleness;
            mfConfig.outputPrefix = cmd.outputPrefix;

            workers[i] = new Thread(new MatrixFactWorker(mfConfig));
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
            logger.info("The program took " + totalTimer.elapsed() + "s.");
        }
    }
}
