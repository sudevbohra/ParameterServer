package org.petuum.app.matrixfact;

import org.petuum.app.matrixfact.Rating;
import org.petuum.app.matrixfact.LossRecorder;
import org.petuum.app.matrixfact.MatrixFactCore;

import org.petuum.ps.PsTableGroup;
import org.petuum.ps.row.double_.DenseDoubleRow;
import org.petuum.ps.row.double_.DenseDoubleRowUpdate;
import org.petuum.ps.row.double_.DoubleRow;
import org.petuum.ps.row.double_.DoubleRowUpdate;
import org.petuum.ps.table.DoubleTable;
import org.petuum.ps.common.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Random;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.IOException;

import java.text.DecimalFormat;

// MatrixFact worker thread logic.
public class MatrixFactWorker implements Runnable {
    private static final Logger logger =
        LoggerFactory.getLogger(MatrixFactWorker.class);

    private int numWorkers;
    private int workerRank;
    private int numThreads;
    private int numEpochs;
    private ArrayList<Rating> ratings;
    private int LTableId;
    private int RTableId;
    private int K;
    private int numRows;
    private int numCols;
    private double lambda;
    private String outputPrefix;

    // Learning rate is learningRateEta0 * learningRateDecay^epoch
    private double learningRateDecay;
    private double learningRateEta0;

    private int numMiniBatchesPerEpoch;

    private int staleness;    // only used for exp record, not algorithm.

    private LossRecorder lossRecorder = new LossRecorder();

    // Partition L and R table for initialization and L2 loss eval. This is
    // not necessarily good for partitioning ratings / entries.
    private int LRowBegin;
    private int LRowEnd;
    private int RRowBegin;
    private int RRowEnd;

    public static class Config {
        public int numWorkers = -1;
        // Worker's rank among all threads across all nodes.
        public int workerRank = -1;
        public int numThreads = -1;
        public int numEpochs = 1;
        public ArrayList<Rating> ratings = null;
        public int LTableId = 0;
        public int RTableId = 1;
        public int K = 20;    // rank
        public int numRows = 0;
        public int numCols = 0;
        public double lambda = 0.1f;
        public double learningRateDecay = 1;
        public double learningRateEta0 = 0.1;
        public int numMiniBatchesPerEpoch =1;
        public int staleness = 0;
        public String outputPrefix = "";
    }

    public MatrixFactWorker(Config config) {
        assert config.numWorkers != -1;
        this.numWorkers = config.numWorkers;

        assert config.workerRank != -1;
        this.workerRank = config.workerRank;

        assert config.numThreads != -1;
        this.numThreads = config.numThreads;

        this.numEpochs = config.numEpochs;

        assert config.ratings != null;
        this.ratings = config.ratings;

        this.LTableId = config.LTableId;
        this.RTableId = config.RTableId;

        this.K = config.K;
        this.numRows = config.numRows;
        this.numCols = config.numCols;

        this.lambda = config.lambda;

        this.learningRateDecay = config.learningRateDecay;
        this.learningRateEta0 = config.learningRateEta0;

        this.numMiniBatchesPerEpoch = config.numMiniBatchesPerEpoch;

        this.outputPrefix = config.outputPrefix;
        this.staleness = config.staleness;

        lossRecorder.registerField("Epoch");
        lossRecorder.registerField("SquareLoss");   // SquareLoss only.
        lossRecorder.registerField("FullLoss");   // SquareLoss + L2Loss.
        lossRecorder.registerField("Time");   // Time elapsed.
        lossRecorder.registerField("NumSamples");   // # aggregated.

        // Each thread takes care of [LRowBegin, LRowEnd) of LTable during
        // initialization and evaluation. Doesn't include LRowEnd.
        int numRowsPerThread = numRows / numWorkers;
        LRowBegin = numRowsPerThread * workerRank;
        LRowEnd = workerRank == numWorkers - 1 ? numRows :
            LRowBegin + numRowsPerThread;

        // Similarly for RTable.
        int numColsPerThread = numCols / numWorkers;
        RRowBegin = numColsPerThread * workerRank;
        RRowEnd = workerRank == numWorkers - 1 ? numCols :
            RRowBegin + numColsPerThread;
    }

    private String printExpDetails() {
        String exp = "";
        exp += "staleness: " + staleness + "\n";
        exp += "numClients: " + numWorkers / numThreads + "\n";
        exp += "numWorkers: " + numWorkers + "\n";
        exp += "numThreads: " + numThreads + "\n";
        exp += "numEpochs: " + numEpochs + "\n";
        exp += "numMiniBatchesPerEpoch: " + numMiniBatchesPerEpoch + "\n";
        exp += "K (rank): " + K + "\n";
        exp += "lambda: " + lambda + "\n";
        exp += "learningRateEta0: " + learningRateEta0 + "\n";
        exp += "learningRateDecay: " + learningRateDecay + "\n";
        exp += "numRows in data: " + numRows + "\n";
        exp += "numCols in data: " + numCols + "\n";
        return exp;
    }

    // Initialize LTable, RTable. Aggregate nnz (# of non-zeros) for
    // [elemBegin, elemEnd).
    private void initLR(DoubleTable LTable, DoubleTable RTable,
            int elemBegin, int elemEnd) {
        Timer initTimer = new Timer();
        Random random = new Random();

        // Aggregate the nnz count.
        int[] nnzLRows = new int[numRows];
        int[] nnzRRows = new int[numCols];

        for (int elem = elemBegin; elem < elemEnd; ++elem) {
            Rating r = ratings.get(elem);
            nnzLRows[r.userId]++;
            nnzRRows[r.prodId]++;
        }

        // column K is the count.
        for (int i = 0; i < numRows; ++i) {
            LTable.inc(i, K, nnzLRows[i]);
        }
        for (int i = 0; i < numCols; ++i) {
            RTable.inc(i, K, nnzRRows[i]);
        }

        for (int i = LRowBegin; i < LRowEnd; i++) {
            DoubleRowUpdate LUpdates = new DenseDoubleRowUpdate(K);
            for (int k = 0; k < K; ++k) {
                // initVal \in (-1, 1)
                //double initVal = random.nextDouble() * 2 - 1;
                //double initVal = random.nextDouble() * 0.02 - 0.01;
                double initVal = random.nextGaussian() * 0.1;
                LUpdates.setUpdate(k, initVal);
            }
            LTable.batchInc(i, LUpdates);
        }

        for (int i = RRowBegin; i < RRowEnd; i++) {
            DoubleRowUpdate RUpdates = new DenseDoubleRowUpdate(K);
            for (int k = 0; k < K; ++k) {
                // initVal \in (-1, 1)
                //double initVal = random.nextDouble() * 2 - 1;
                //double initVal = random.nextDouble() * 0.02 - 0.01;
                double initVal = random.nextGaussian() * 0.1;
                RUpdates.setUpdate(k, initVal);
            }
            RTable.batchInc(i, RUpdates);
        }
        if (workerRank == 0) {
            logger.info("Initialized L,R in " + initTimer.elapsed() + "s");
        }
    }

    // Output CSV format.
    private void outputCsvToDisk(String outputPrefix) {
        Timer diskTimer = new Timer();
        DoubleTable LTable = PsTableGroup.getDoubleTableOrDie(LTableId);
        DoubleTable RTable = PsTableGroup.getDoubleTableOrDie(RTableId);
        DecimalFormat doubleFormat = new DecimalFormat("0.###E0");

        // Write L
        PrintWriter out = null;
        try {
            out = new PrintWriter(new BufferedWriter(
                        new FileWriter(outputPrefix + ".L.csv")));
        } catch (IOException e) {
            logger.error("Caught IOException: " + e.getMessage());
        }
        DoubleRow rowCache = new DenseDoubleRow(K);
        for (int i = 0; i < numRows; ++i) {
            DoubleRow LRow = LTable.get(i);
            rowCache.reset(LRow);
            for (int k = 0; k < K - 1; ++k) {
                out.print(doubleFormat.format(rowCache.getUnlocked(k)) + ",");
            }
            out.println(rowCache.getUnlocked(K-1));   // no comma
        }
        out.close();

        // Write R
        try {
            out = new PrintWriter(new BufferedWriter(
                        new FileWriter(outputPrefix + ".R.csv")));
        } catch (IOException e) {
            logger.error("Caught IOException: " + e.getMessage());
        }
        for (int i = 0; i < numCols; ++i) {
            DoubleRow RRow = RTable.get(i);
            rowCache.reset(RRow);
            for (int k = 0; k < K - 1; ++k) {
                out.print(doubleFormat.format(rowCache.getUnlocked(k)) + ",");
            }
            out.println(rowCache.getUnlocked(K-1));   // no comma
        }
        out.close();
        logger.info("Finish outputing to " + outputPrefix + " in "
                + diskTimer.elapsed() + " sec");
    }

    // Main worker thread logic.
    @Override
    public void run() {
        // Let PS know this thread wants to interact with it.
        PsTableGroup.registerThread();

        // Get table by ID.
        DoubleTable LTable = PsTableGroup.getDoubleTableOrDie(LTableId);
        DoubleTable RTable = PsTableGroup.getDoubleTableOrDie(RTableId);

        int numElemsPerWorker = ratings.size() / numWorkers;
        int elemBegin = workerRank * numElemsPerWorker;
        int elemEnd = (workerRank == numWorkers - 1) ?
            ratings.size() : elemBegin + numElemsPerWorker;
        // Further divide workload into minibatches.
        int numElemsPerMiniBatch = numElemsPerWorker / numMiniBatchesPerEpoch;
        if (workerRank == 0) {
            logger.info("numElemsPerWorker: " + numElemsPerWorker +
                    "; numElemsPerMiniBatch: " + numElemsPerMiniBatch +
                    " (numMiniBatchesPerEpoch: " + numMiniBatchesPerEpoch
                    + ")");
        }

        // Since each thread initialize part of L,R table, use barrier to
        // ensure initialization completes.
        initLR(LTable, RTable, elemBegin, elemEnd);
        PsTableGroup.globalBarrier();

        // Elements assigned to other machine. We will evaluate on other
        // node's data since local stale parameter is "overfitted" to the
        // ratings in a worker's data partition.
        int workerRankOther = (workerRank + numThreads) % numWorkers;
        int elemBeginOther = workerRankOther * numElemsPerWorker;
        int elemEndOther = (workerRankOther == numWorkers - 1) ?
            ratings.size() : elemBeginOther + numElemsPerWorker;

        Timer trainTimer = new Timer();
        int evalCounter = 0;
        for (int epoch = 1; epoch <= numEpochs; epoch++) {
            Timer epochTimer = new Timer();
            double learningRate = learningRateEta0 *
                Math.pow(learningRateDecay, epoch - 1);
            for (int batch = 0; batch < numMiniBatchesPerEpoch; ++batch) {
                int elemMiniBatchBegin = elemBegin +
                    batch * numElemsPerMiniBatch;
                int elemMiniBatchEnd = (batch == numMiniBatchesPerEpoch - 1) ?
                    elemEnd : elemMiniBatchBegin + numElemsPerMiniBatch;
                for (int ratingId = elemMiniBatchBegin;
                        ratingId < elemMiniBatchEnd; ratingId++) {
                    Rating r = ratings.get(ratingId);
                    MatrixFactCore.sgdOneRating(r, learningRate, LTable,
                            RTable, K, lambda);
                }
                PsTableGroup.clock();   // clock every miniBatch.
            }

            // Evaluate loss every epoch. We evaluate loss on elements
            // assigned to other workers to avoid local stale parameters
            // underestimating the loss.
            Timer evalTimer = new Timer();
            MatrixFactCore.evaluateLoss(ratings, evalCounter, elemBeginOther,
                    elemEndOther, LTable, RTable, LRowBegin, LRowEnd,
                    RRowBegin, RRowEnd, lossRecorder, K, lambda);
            if (workerRank == 0) {
                logger.info("Finish eval " + evalCounter + " in "
                        + evalTimer.elapsed());
            }
            if (workerRank == 0) {
                lossRecorder.incLoss(evalCounter, "Epoch", epoch);
                lossRecorder.incLoss(evalCounter, "Time",
                        trainTimer.elapsed());
                // Print last eval's result.
                if (evalCounter > 0) {
                    logger.info(lossRecorder.printOneLoss(evalCounter - 1));
                }
            }
            evalCounter++;
            if (workerRank == 0) {
                logger.info("Epoch " + epoch + "; took: "
                        + epochTimer.elapsed() + "s");
            }
        }
        PsTableGroup.globalBarrier();

        // Print all results.
        if (workerRank == 0) {
            logger.info("\n" + printExpDetails() + "\n" +
                    lossRecorder.printAllLoss());
            if (!outputPrefix.equals("")) {
                outputCsvToDisk(outputPrefix);
            }
        }

        // Tell PS I'm done for ordered shutdown.
        PsTableGroup.deregisterThread();
    }
}
