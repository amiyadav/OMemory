package org.amity.concurrency.executors;

import org.amity.concurrency.error.RejectedExecutionHandlerImpl;
import org.amity.concurrency.factory.ThreadFactoryBuilder;
import org.amity.concurrency.worker.WorkerThread;
import org.amity.monitoring.PartitionsMonitorThread;

import java.util.Map;
import java.util.concurrent.*;

/**
 *
 */
public class PartitionWriterThreadExecutor implements ThreadExecutorPool {

    // we can adjust thread pool size dynamically
    private static final String PARTITION_WRITE_THREAD = "pw-thread";
    private static ThreadPoolExecutor executorPool;
    private final int threadPoolSize;
    Map<String, Integer> threadNameToPartition;

    public PartitionWriterThreadExecutor(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
        this.threadNameToPartition = new ConcurrentHashMap<>();
    }

    public void initExecutorPool() {
        //RejectedExecutionHandler implementation
        RejectedExecutionHandlerImpl rejectionHandler = new RejectedExecutionHandlerImpl();
        //Get the ThreadFactory implementation to use
//        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        ThreadFactory customThreadfactory = new ThreadFactoryBuilder()
                .setNamePrefix(PARTITION_WRITE_THREAD)
                .setDaemon(false)
                .setPriority(Thread.MAX_PRIORITY)
                .setUncaughtExceptionHandler((t, e) -> System.err.printf(
                        "Thread %s threw exception - %s%n", t.getName(),
                        e.getMessage())).build();

        //creating the ThreadPoolExecutor
        executorPool = new ThreadPoolExecutor(
                2, threadPoolSize, 10, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(2), customThreadfactory, rejectionHandler);

        // start monitor thread
        startMonitorThread();

        //submit work to the thread pool
        for (int i = 0; i < threadPoolSize - 1; i++) {
            threadNameToPartition.putIfAbsent(PARTITION_WRITE_THREAD + "-" + i, i);
            executorPool.execute(new WorkerThread(i));
        }
    }


    @Override
    public void startMonitorThread() {

        //start the monitoring thread
        PartitionsMonitorThread monitor = new PartitionsMonitorThread(executorPool, 3);
        Thread monitorThread = new Thread(monitor);
        // make it daemon to continuously running in background
        monitorThread.setDaemon(true);
        monitorThread.start();


//        Thread.sleep(30000);
        //shut down the pool
//        executorPool.shutdown();
        //shut down the monitor thread
//        Thread.sleep(5000);
//        monitor.shutdown();
    }

    @Override
    public int getType() {
        return 0;
    }

    @Override
    public void shutdown() {
        executorPool.shutdown();
    }
}
