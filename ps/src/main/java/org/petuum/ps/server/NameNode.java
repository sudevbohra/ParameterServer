package org.petuum.ps.server;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class NameNode {

    private static NameNodeThread nameNodeThread;
    private static CyclicBarrier initBarrier;

    public static void init() {
        initBarrier = new CyclicBarrier(2);
        nameNodeThread = new NameNodeThread(initBarrier);
        nameNodeThread.start();
        try {
            initBarrier.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
    }

    public static void shutdown() {
        nameNodeThread.shutDown();
    }

}
