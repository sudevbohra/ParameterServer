package org.petuum.ps.client.thread;

import org.petuum.ps.common.util.IntBox;

import java.util.List;

public abstract class RowRequestOpLogMgr {

    public abstract boolean addRowRequest(RowRequestInfo request, int tableId,
                                          int rowId);

    public abstract int informReply(int tableId, int rowId, int clock,
                                    int currVersion, List<Integer> appThreadIds);

    public abstract BgOpLog getOpLog(int version);

    public abstract void informVersionInc();

    public abstract void serverAcknowledgeVersion(int serverId, int version);

    public abstract boolean addOpLog(int version, BgOpLog oplog);

    public abstract BgOpLog opLogIterInit(int startVersion, int endVersion);

    public abstract BgOpLog opLogIterNext(IntBox version);

}
