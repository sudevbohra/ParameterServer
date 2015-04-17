package org.petuum.ps.row;

import java.nio.ByteBuffer;

/**
 * Created by aqiao on 2/20/15.
 */
public interface RowUpdate {
    public RowUpdate getCopy();

    public void addRowUpdate(RowUpdate rowUpdate);

    public int getSerializedSize();

    public ByteBuffer serialize();

    public void clear();
}