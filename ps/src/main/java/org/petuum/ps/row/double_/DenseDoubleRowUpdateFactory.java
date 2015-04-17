package org.petuum.ps.row.double_;

import org.petuum.ps.config.Config;
import org.petuum.ps.config.ConfigKey;
import org.petuum.ps.row.RowUpdate;
import org.petuum.ps.row.RowUpdateFactory;

import java.nio.ByteBuffer;

/**
 * Created by aqiao on 2/23/15.
 */
public class DenseDoubleRowUpdateFactory implements RowUpdateFactory {
    @Override
    public RowUpdate create(Config config) {
        return new DenseDoubleRowUpdate(config.getInt(ConfigKey.DENSE_ROW_UPDATE_CAPACITY));
    }

    @Override
    public RowUpdate deserialize(ByteBuffer data) {
        return new DenseDoubleRowUpdate(data);
    }
}