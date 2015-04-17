package org.petuum.ps.server;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import org.petuum.ps.client.thread.GlobalContext;
import org.petuum.ps.common.util.IntBox;
import org.petuum.ps.common.util.RecordBuff;

import java.nio.ByteBuffer;
import java.util.BitSet;

public class CallBackSubs {

    private static final int K_PETUUM_MAX_NUM_CLIENTS = 8;
    private BitSet subscriptions_;

    public CallBackSubs() {
        subscriptions_ = new BitSet(K_PETUUM_MAX_NUM_CLIENTS);
    }

    public void close() {
    }

    public boolean Subscribe(int client_id) {
        boolean bit_changed = false;
        if (!subscriptions_.get(client_id)) {
            bit_changed = true;
            subscriptions_.set(client_id);
        }
        return bit_changed;
    }

    public boolean Unsubscribe(int client_id) {
        boolean bit_changed = false;
        if (subscriptions_.get(client_id)) {
            bit_changed = true;
            subscriptions_.clear(client_id);
        }
        return bit_changed;
    }

    public boolean AppendRowToBuffs(int client_id_st,
                                    TIntObjectMap<RecordBuff> buffs, ByteBuffer row_data, int row_size,
                                    int row_id, IntBox failed_client_id) {
        // Some simple tests show that iterating bitset isn't too bad.
        // For bitset size below 512, it takes 200~300 ns on an Intel i5 CPU.
        int client_id;
        for (client_id = client_id_st; client_id < GlobalContext
                .getNumClients(); ++client_id) {
            if (subscriptions_.get(client_id)) {
                boolean suc = buffs.get(client_id).Append(row_id, row_data,
                        row_size);
                if (!suc) {
                    failed_client_id.intValue = client_id;
                    return false;
                }
            }
        }
        return true;
    }

    public void AccumSerializedSizePerClient(TIntIntMap client_size_map,
                                             int serialized_size) {
        int client_id;
        for (client_id = 0; client_id < GlobalContext.getNumClients(); ++client_id) {
            if (subscriptions_.get(client_id)) {
                int client_size = client_size_map.get(client_id);
                client_size += serialized_size + 4 + 4;
                client_size_map.put(client_id, client_size);
            }
        }
    }

    void AppendRowToBuffs(TIntObjectMap<RecordBuff> buffs, ByteBuffer row_data,
                          int row_size, int row_id) {
        int client_id;
        for (client_id = 0; client_id < GlobalContext.getNumClients(); ++client_id) {
            if (subscriptions_.get(client_id)) {
                boolean suc = buffs.get(client_id).Append(row_id, row_data,
                        row_size);
                if (!suc) {
                    buffs.get(client_id).PrintInfo();
                }
            }
        }
    }
}
