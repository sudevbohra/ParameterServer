package org.petuum.ps.client.thread;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.petuum.ps.common.util.IntBox;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

public class SSPRowRequestOpLogMgr extends RowRequestOpLogMgr {

    private Table<Integer, Integer, LinkedList<RowRequestInfo>> pendingRowRequests;
    private TIntObjectMap<BgOpLog> versionOplogMap;
    private TIntIntMap versionRequestCntMap;
    private int oplogIterVersionNext;
    private int oplogIterVersionSt;
    private int oplogIterVersionEnd;

    public SSPRowRequestOpLogMgr() {
        pendingRowRequests = HashBasedTable.create();
        versionOplogMap = new TIntObjectHashMap<>();
        versionRequestCntMap = new TIntIntHashMap();
    }

    @Override
    public boolean addRowRequest(RowRequestInfo request, int tableId, int rowId) {
        int version = request.version;
        request.sent = true;
        if (!pendingRowRequests.contains(tableId, rowId)) {
            pendingRowRequests.put(tableId, rowId,
                    new LinkedList<RowRequestInfo>());
        }
        LinkedList<RowRequestInfo> requestList = pendingRowRequests.get(
                tableId, rowId);
        boolean requestAdded = false;
        // Requests are sorted in increasing order of clock number.
        // When a request is to be inserted, start from the end as the request's
        // clock is more likely to be larger.
        int clock = request.clock;
        //if (tableId == 1 && rowId == 2)
        // System.out.println("addRowRequest " + this + " tableId=" + tableId +
        // " rowId=" + rowId + " version=" + version + " clock=" + clock);
        ListIterator<RowRequestInfo> iter = requestList
                .listIterator(requestList.size());
        while (iter.hasPrevious()) {
            RowRequestInfo prev = iter.previous();
            if (clock >= prev.clock) {
                request.sent = false;
                iter.next();
                iter.add(request);
                requestAdded = true;
                break;
            }
        }
        if (!requestAdded) {
            requestList.addFirst(request);
        }
        if (!versionRequestCntMap.containsKey(version)) {
            versionRequestCntMap.put(version, 0);
        }

        versionRequestCntMap.increment(version);

        // System.out.println("versionRequestCntMap.size()=" +
        // versionRequestCntMap.size() + " request.sent=" + request.sent);

        return request.sent;
    }

    @Override
    public int informReply(int tableId, int rowId, int clock, int currVersion,
                           List<Integer> appThreadIds) {
        //if (tableId == 1 && rowId == 2)
        // System.out.println("informReply " + this + " tableId=" + tableId +
        // " rowId=" + rowId + " currVersion=" + currVersion + " clock=" +
        // clock);
        appThreadIds.clear();
        LinkedList<RowRequestInfo> requestList = pendingRowRequests.get(
                tableId, rowId);
        int clockToRequest = -1;

        boolean satisfiedSent = false;
        while (requestList != null && !requestList.isEmpty()) {
            RowRequestInfo request = requestList.getFirst();
            if (request.clock <= clock && !satisfiedSent) {
                //if (tableId == 1 && rowId == 2)
                //    System.out.println("Satisfied request tableId=" + tableId + " rowId=" + rowId + " request.clock=" + request.clock + " clock=" + clock);
                // remove the request
                int reqVersion = request.version;
                appThreadIds.add(request.appThreadId);
                requestList.pollFirst();

                //if (tableId == 1 && rowId == 2)
                //    System.out.println("requestList Size=" + requestList.size());

                // decrement the version count
                versionRequestCntMap.adjustValue(reqVersion, -1);
                Preconditions.checkArgument(versionRequestCntMap
                        .get(reqVersion) >= 0);
                // if version count becomes 0, remove the count
                if (versionRequestCntMap.get(reqVersion) == 0) {
                    versionRequestCntMap.remove(reqVersion);
                    cleanVersionOpLogs(reqVersion, currVersion);
                }
                if (request.sent) {
                    satisfiedSent = true;
                }
            } else {
                if (!request.sent) {
                    clockToRequest = request.clock;
                    request.sent = true;
                    int reqVersion = request.version;
                    versionRequestCntMap.adjustValue(reqVersion, -1);

                    request.version = currVersion - 1;
                    if (!versionRequestCntMap.containsKey(request.version)) {
                        versionRequestCntMap
                                .put(request.version, 0);
                    }
                    versionRequestCntMap.increment(request.version);

                    if (versionRequestCntMap.get(reqVersion) == 0) {
                        versionRequestCntMap.remove(reqVersion);
                        cleanVersionOpLogs(reqVersion, currVersion);
                    }
                }
                break;
            }
        }
        // System.out.println("versionRequestCntMap.size()=" +
        // versionRequestCntMap.size());
        // if there's no request in that list, I can remove the empty list
        if (requestList != null && requestList.isEmpty())
            pendingRowRequests.remove(tableId, rowId);
        return clockToRequest;
    }

    @Override
    public BgOpLog getOpLog(int version) {
        Preconditions.checkArgument(versionOplogMap.containsKey(version));
        return versionOplogMap.get(version);
    }

    @Override
    public void informVersionInc() {
        // not supported

    }

    @Override
    public void serverAcknowledgeVersion(int serverId, int version) {
        // not supported
    }

    @Override
    public boolean addOpLog(int version, BgOpLog oplog) {
        //System.out.println("addOpLog " + this + " version=" + version);
        Preconditions.checkArgument(!versionOplogMap.containsKey(version));
        // System.out.println("versionRequestCntMap.size()=" +
        // versionRequestCntMap.size());
        // There are pending requests, they are from some older version or the
        // current
        // version, so I need to save the oplog for them.
        if (versionRequestCntMap.size() > 0) {
            versionOplogMap.put(version, oplog);
            // System.out.println("versionOplogMap add version=" + version);
            return true;
        }
        return false;
    }

    @Override
    public BgOpLog opLogIterInit(int startVersion, int endVersion) {
        oplogIterVersionSt = startVersion;
        oplogIterVersionEnd = endVersion;
        oplogIterVersionNext = oplogIterVersionSt + 1;
        return getOpLog(startVersion);
    }

    @Override
    public BgOpLog opLogIterNext(IntBox version) {
        if (oplogIterVersionNext > oplogIterVersionEnd)
            return null;
        version.intValue = oplogIterVersionNext;
        ++oplogIterVersionNext;
        return getOpLog(version.intValue);
    }

    private void cleanVersionOpLogs(int reqVersion, int currVersion) {
        //System.out.println("cleanVersionOpLogs " + this + " reqVersion=" +
        //reqVersion + " currVersion=" + currVersion);
        // All oplogs that are saved must be of an earlier version than current
        // version, while a request could be from the current version.

        // The first version to be removed is current version, which is not yet
        // stored
        // So nothing to remove.
        if (reqVersion + 1 == currVersion)
            return;

        // First, make sure there's no request from a previous version.
        // We do that by checking if there's an OpLog of this version,
        // if there is one, it must be save for some older requests.
        if (versionOplogMap.containsKey(reqVersion))
            return;

        int versionToRemove = reqVersion;
        do {
            // No previous OpLog, can remove a later version of oplog.
            versionOplogMap.remove(versionToRemove + 1);
            // System.out.println("versionOplogMap remove version=" +
            // (versionToRemove + 1));
            ++versionToRemove;
            // Figure out how many later versions of oplogs can be removed.
        } while ((!versionRequestCntMap.containsKey(versionToRemove))
                && (versionToRemove != currVersion));
    }
}
