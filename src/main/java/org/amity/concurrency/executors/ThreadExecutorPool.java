package org.amity.concurrency.executors;

/**
 *
 */
public interface ThreadExecutorPool {

    /**
     * initialize executor with thread pool size and also start monitor
     */
    void initExecutorPool();

    /**
     * Start monitor thread to check thread pool stats
     */
    void startMonitorThread();

    /**
     * provide thread executor pool type
     * 0- for write
     * 1- for read
     * 2- for monitor
     *
     * @return thread executor pool type type
     */
    int getType();

    /**
     * shutdown executor
     */
    void shutdown();

}
