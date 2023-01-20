package org.amity.monitoring;

import java.util.concurrent.ThreadPoolExecutor;

public class PartitionsMonitorThread implements Runnable {
    private final ThreadPoolExecutor executor;
    private final long seconds;
    private boolean run = true;

    public PartitionsMonitorThread(ThreadPoolExecutor executor, long delay) {
        this.executor = executor;
        this.seconds = delay;
    }

    public void shutdown() {
        this.run = false;
    }

    @Override
    public void run() {
        while (run) {
            System.out.printf("[monitor] [%d/%d] Active: %d, Completed: %d, Task: %d, isShutdown: %s, isTerminated: %s%n",
                    this.executor.getPoolSize(),
                    this.executor.getCorePoolSize(),
                    this.executor.getActiveCount(),
                    this.executor.getCompletedTaskCount(),
                    this.executor.getTaskCount(),
                    this.executor.isShutdown(),
                    this.executor.isTerminated());
            try {
                Thread.sleep(seconds * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}