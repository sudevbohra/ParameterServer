package org.petuum.ps.server;

import org.petuum.ps.client.thread.GlobalContext;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class ServerThreadGroup {

    private ServerThread[] server_thread_vec_;
    private CyclicBarrier init_barrier_;

    public ServerThreadGroup(int server_id_st) {
        server_thread_vec_ = new ServerThread[GlobalContext
                .getNumCommChannelsPerClient()];
        init_barrier_ = new CyclicBarrier(
                GlobalContext.getNumCommChannelsPerClient() + 1);

        for (int idx = 0; idx < GlobalContext.getNumCommChannelsPerClient(); idx++) {
            server_thread_vec_[idx] = new ServerThread(
                    GlobalContext.getServerThreadId(
                            GlobalContext.getClientId(), idx), init_barrier_);
        }

    }

    public void start() {
        for (ServerThread server_thread : server_thread_vec_) {
            server_thread.start();
        }

        try {
            init_barrier_.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
    }

    public void shutdown() {
        for (ServerThread server_thread : server_thread_vec_) {
            server_thread.shutDown();
        }
    }

}
