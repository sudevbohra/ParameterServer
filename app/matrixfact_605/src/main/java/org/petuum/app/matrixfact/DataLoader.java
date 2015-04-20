package org.petuum.app.matrixfact;

import org.petuum.app.matrixfact.Rating;

import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;

import java.util.ArrayList;

public class DataLoader {
    // Return dimension [numRows, numCols]
    public static int[] ReadData(String dataFile, ArrayList<Rating> X) {
        X.clear();
        int[] dim = new int[2];
        try (BufferedReader br = new BufferedReader(new FileReader(dataFile))) {
            String currLine;
            while ((currLine = br.readLine()) != null) {
                String[] fields = currLine.split(" ");
                int row = Integer.parseInt(fields[0]);
                int col = Integer.parseInt(fields[1]);
                float val = Float.parseFloat(fields[2]);
                // +1 because row, col indices are 0-based.
                dim[0] = row + 1 > dim[0] ? row + 1 : dim[0];
                dim[1] = col + 1 > dim[1] ? col + 1 : dim[1];
                X.add(new Rating(row, col, val));
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return dim;
    }
}
