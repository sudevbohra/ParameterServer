package org.petuum.app.matrixfact;

import org.petuum.app.matrixfact.Rating;
import org.petuum.app.matrixfact.LossRecorder;

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
import java.lang.math.*;

public class MatrixFactCore {
    private static final Logger logger =
        LoggerFactory.getLogger(MatrixFactCore.class);

    // Perform a single SGD on a rating and update LTable and RTable
    // accordingly.
    // public Rating(int userId, int prodId, float rating) {
    //     this.userId = userId;
    //     this.prodId = prodId;
    //     this.rating = rating;
    // }
    public static void sgdOneRating(Rating r, double learningRate,
            DoubleTable LTable, DoubleTable RTable, int K, double lambda) {
        // TODO
        // Get L[row,:] (row is users)
        DoubleRow lRow = new DenseDoubleRow(K+1);
        DoubleRow lRowGet = LTable.get(r.userId);
        lRow.reset(lRowGet);

        // Get R[:,col] (col is movies)

        DoubleRow rRow = new DenseDoubleRow(K+1);
        DoubleRow rRowGet = RTable.get(r.prodId);
        rRow.reset(rRowGet);
        // Get value
        float value = r.rating;
        // Get Nrow and Ncol
        double n = lRow.getUnlocked(K);
        double m =  rRow.getUnlocked(K);
        //calculate gradientL and gradientR
        double e_ij = value - dot(lRow,rRow);

        DoubleRowUpdate gradientL_updates = new DenseDoubleRowUpdate(K+1);
        for (int i = 0; i < K; i++) {
            double delta = 2*learningRate*(e_ij*rRow.getUnlocked(i)-(lambda/n)*lRow.getUnlocked(i));
            gradientL_updates.setUpdate(i, delta);
        }
        lTable.batchInc(r.userId, gradientL_updates);

        DoubleRowUpdate gradientR_updates = new DenseDoubleRowUpdate(K+1);
        for (int i = 0; i < K; i++) {
            double delta = 2*learningRate*(e_ij*lRow.getUnlocked(i)-(lambda/m)*rRow.getUnlocked(i));
            gradientR_updates.setUpdate(i, delta);
        }
        rTable.batchInc(r.prodId, gradientR_updates);
        // add gradient*learning to L and R
    }


    public static double dot(DoubleRow lRow, DoubleRow rRow, int K) {
        float sum = 0;
        for (int i = 0; i < K; i++) {
            sum += lRow.getUnlocked(i)*rRow.getUnlocked(i);
        }

        return sum;
    }

    // Evaluate square loss on entries [elemBegin, elemEnd), and L2-loss on of
    // row [LRowBegin, LRowEnd) of LTable,  [RRowBegin, RRowEnd) of Rtable.
    // Note the interval does not include LRowEnd and RRowEnd. Record the loss to
    // lossRecorder.
    public static void evaluateLoss(ArrayList<Rating> ratings, int ithEval,
            int elemBegin, int elemEnd, DoubleTable LTable,
            DoubleTable RTable, int LRowBegin, int LRowEnd, int RRowBegin,
            int RRowEnd, LossRecorder lossRecorder, int K, double lambda) {
        // TODO

        //Square loss:
        double sqLoss = 0;
        for (int i = elemBegin; i < elemEnd; i++) {
            Rating r = ratings[i];
            DoubleRow lRow = new DenseDoubleRow(K+1);
            DoubleRow lRowGet = LTable.get(r.userId);
            lRow.reset(lRowGet);

            // Get R[:,col] (col is movies)

            DoubleRow rRow = new DenseDoubleRow(K+1);
            DoubleRow rRowGet = RTable.get(r.prodId);
            rRow.reset(rRowGet);

            double p = dot(lRow,rRow);
            double diff = r.rating -p;
            sqLoss += (diff*diff);
        }

        double totalLoss = sqLoss;
        totalLoss += lambda*(sqFrobenius(LTable, K) + sqFrobenius(RTable, K));
      
        lossRecorder.incLoss(ithEval, "SquareLoss", sqLoss);
        lossRecorder.incLoss(ithEval, "FullLoss", totalLoss);
        lossRecorder.incLoss(ithEval, "NumSamples", elemEnd - elemBegin);
    }

    public static double sqFrobenius(DoubleTable table, int K){
        double sum = 0;

        for (int i = 0; i < K; i++) {
            DoubleRow lRow = new DenseDoubleRow(K+1);
            DoubleRow lRowGet = table.get(i);
            lRow.reset(lRowGet);

            for (int j = 0; j < K; j++) {
                double val = lRow.getUnlocked(j);
                sum += val*val;
            }
        }

        return sum;
    }

}
