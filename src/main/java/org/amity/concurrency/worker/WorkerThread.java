package org.amity.concurrency.worker;

public class WorkerThread extends Thread {

    private final int partitionNumber;

    public WorkerThread(int partitionNumber) {
        this.partitionNumber = partitionNumber;
    }

    @Override
    public void run() {

        // TODO Start writing sequentially to its own assigned partition
        // validate the length of record as We are expecting fixed length record
        //
        processCommand();
    }

    private void processCommand() {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "start writing on partition :" + String.valueOf(this.partitionNumber);
    }
}