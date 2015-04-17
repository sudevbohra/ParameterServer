package org.petuum.ps.row.double_;

import org.petuum.ps.config.Config;
import org.petuum.ps.config.ConfigKey;
import org.petuum.ps.row.Row;
import org.petuum.ps.row.RowFactory;

/**
 * Created by aqiao on 2/23/15.
 */
public class DenseDoubleRowFactory implements RowFactory {
    @Override
    public Row createRow(Config config) {
        return new DenseDoubleRow(config.getInt(ConfigKey.DENSE_ROW_CAPACITY));
    }
}
