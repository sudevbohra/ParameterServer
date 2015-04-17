package org.petuum.ps.row;

import org.petuum.ps.config.Config;

import java.nio.ByteBuffer;

/**
 * Created by aqiao on 2/23/15.
 */
public interface RowUpdateFactory {
    public RowUpdate create(Config config);

    public RowUpdate deserialize(ByteBuffer data);
}
