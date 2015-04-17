package org.petuum.ps.row.long_;

import org.petuum.ps.config.Config;
import org.petuum.ps.config.ConfigKey;
import org.petuum.ps.row.Row;
import org.petuum.ps.row.RowFactory;

/**
 * Created by aqiao on 2/23/15.
 */
public class DenseLongRowFactory implements RowFactory {
    @Override
    public Row createRow(Config config) {
        return new DenseLongRow(config.getInt(ConfigKey.DENSE_ROW_CAPACITY));
    }
}
