package org.petuum.ps.row;

import org.petuum.ps.config.Config;

/**
 * Created by aqiao on 2/23/15.
 */
public interface RowFactory {
    public Row createRow(Config rowConfig);
}
