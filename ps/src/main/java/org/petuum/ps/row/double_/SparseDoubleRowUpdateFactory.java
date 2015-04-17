package org.petuum.ps.row.double_;

import org.petuum.ps.config.Config;
import org.petuum.ps.row.RowUpdate;
import org.petuum.ps.row.RowUpdateFactory;

import java.nio.ByteBuffer;

/**
 * Created by aqiao on 3/4/15.
 */
public class SparseDoubleRowUpdateFactory implements RowUpdateFactory {
    @Override
    public RowUpdate create(Config config) {
        return new SparseDoubleRowUpdate();
    }

    @Override
    public RowUpdate deserialize(ByteBuffer data) {
        return new SparseDoubleRowUpdate(data);
    }
}
