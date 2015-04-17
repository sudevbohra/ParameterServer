package org.petuum.ps.common.util;

public class Timer {

    private long start_time;

    public Timer() {
        restart();
    }

    public void restart() {
        start_time = System.currentTimeMillis();
    }

    public float elapsed() {
        return (float) (System.currentTimeMillis() - start_time) / 1e3f;
    }
}
