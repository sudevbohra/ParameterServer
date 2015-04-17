package org.petuum.app.matrixfact;

import org.petuum.ps.PsTableGroup;
import org.petuum.ps.config.*;
import org.petuum.ps.row.double_.DenseDoubleRow;
import org.petuum.ps.row.double_.DenseDoubleRowUpdate;
import org.petuum.ps.row.double_.DoubleRow;
import org.petuum.ps.row.double_.DoubleRowUpdate;
import org.petuum.ps.table.DoubleTable;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.text.DecimalFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LossRecorder {
    private static final Logger logger =
        LoggerFactory.getLogger(LossRecorder.class);

    public static final int kMaxNumFields = 100; // Track at most 100 fields.
    public static final int kCacheSize = 1000;
    public static final int kLossTableId = 999;

    private DoubleTable lossTable;
    private int numFields = 0;
    // The index of a field is its column idx in lossTable.
    private ArrayList<String> fields = new ArrayList<String>();
    private int maxEval = 0;

    // This needs to be called before PsTableGroup.createTableDone().
    public static void createLossTable() {
        TableConfig tableConfig = new TableConfig();
        tableConfig.setStaleness(0)
            .setProcessCacheCapacity(kCacheSize);
        PsTableGroup.createDenseDoubleTable(kLossTableId, kMaxNumFields,
                tableConfig);
    }

    public LossRecorder() {
        lossTable = PsTableGroup.getDoubleTableOrDie(kLossTableId);
    }

    // All workers need to register fields in the same order.
    public void registerField(String fieldname) {
        fields.add(fieldname);
        numFields++;
    }

    // Return -1 if not found.
    private int findField(String fieldname) {
        for (int j = 0; j < numFields; ++j) {
            if (fields.get(j).equals(fieldname)) {
                return j;
            }
        }
        return -1;
    }

    public void incLoss(int ith, String fieldname, double val) {
        int fieldIdx = findField(fieldname);
        assert fieldIdx != -1;
        maxEval = Math.max(ith, maxEval);
        lossTable.inc(ith, fieldIdx, val);
    }

    public String printAllLoss() {
        // Print header.
        String header = "";
        for (int j = 0; j < numFields; ++j) {
            header += fields.get(j) + " ";
        }
        header += "\n";

        // Print each row.
        DecimalFormat doubleFormat = new DecimalFormat("#.00");
        DecimalFormat intFormat = new DecimalFormat("#");
        String stats = "";
        for (int i = 0; i <= maxEval; ++i) {
            DoubleRow lossRow = lossTable.get(i);
            for (int j = 0; j < numFields; ++j) {
                double val = lossRow.get(j);
                String formatVal = (val % 1 == 0) ? intFormat.format(val) :
                    doubleFormat.format(val);
                stats += formatVal + " ";
                assert !Double.isNaN(val);
            }
            stats += "\n";
        }
        return header + stats;
    }

    public String printOneLoss(int ith) {
        DoubleRow lossRow = lossTable.get(ith);
        String stats = "";
        DecimalFormat doubleFormat = new DecimalFormat("#.00");
        DecimalFormat intFormat = new DecimalFormat("#");
        for (int j = 0; j < numFields; ++j) {
            double val = lossRow.get(j);
            String formatVal = (val % 1 == 0) ? intFormat.format(val) :
                doubleFormat.format(val);
            stats += fields.get(j) + ": " + formatVal + " ";
            assert !Double.isNaN(val);
        }
        return stats;
    }
}
