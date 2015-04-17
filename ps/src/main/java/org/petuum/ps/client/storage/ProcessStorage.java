package org.petuum.ps.client.storage;

import org.petuum.ps.client.ClientRow;

/**
 * Created by aqiao on 2/23/15.
 */
public interface ProcessStorage {
    public ClientRow getRow(int rowId);

    public void putRow(int rowId, ClientRow row);
}