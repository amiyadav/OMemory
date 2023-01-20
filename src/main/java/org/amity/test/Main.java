package org.amity.test;

import java.io.File;
import java.lang.management.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class Main {

    public static void main(String[] args) {
        int numOfCores = Runtime.getRuntime().availableProcessors();
        System.out.println(numOfCores);

        System.out.println(IntStream.range(0, 10).filter(x -> x/2==0).findFirst());

        //        IntStream.range(0, 10).forEach(x -> {
//            System.out.println(x);
//        });

//        FileSystemView fsv = FileSystemView.getFileSystemView();
//
//        File[] drives = File.listRoots();
//        Collections.singletonList(drives[0]).forEach(aDrive -> {
//                    System.out.println("Drive Letter: " + aDrive);
//                    System.out.println("\tType: " + fsv.getSystemTypeDescription(aDrive));
//                    System.out.println("\tTotal space: " + aDrive.getTotalSpace());
//                    System.out.println("\tFree space: " + aDrive.getFreeSpace());
//                    System.out.println();
//                }
//
//        );

        // latest
//        File root = new File("/");
//        System.out.println(String.format("Total space: %.2f GB",
//                (double) root.getTotalSpace() / 1073741824));
//        System.out.println(String.format("Free space: %.2f GB",
//                (double) root.getFreeSpace() / 1073741824));
//        System.out.println(String.format("Usable space: %.2f GB",
//                (double) root.getUsableSpace() / 1073741824));
//
//        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
//        System.out.println(String.format("Initial memory: %.2f GB",
//                (double) memoryMXBean.getHeapMemoryUsage().getInit() / 1073741824));
//        System.out.println(String.format("Used heap memory: %.2f GB",
//                (double) memoryMXBean.getHeapMemoryUsage().getUsed() / 1073741824));
//        System.out.println(String.format("Max heap memory: %.2f GB",
//                (double) memoryMXBean.getHeapMemoryUsage().getMax() / 1073741824));
//        System.out.println(String.format("Committed memory: %.2f GB",
//                (double) memoryMXBean.getHeapMemoryUsage().getCommitted() / 1073741824));
//        System.out.println(String.format("Available memory: %.2f GB",
//                (double) Runtime.getRuntime().totalMemory() / 1073741824));
//
//        for (MemoryPoolMXBean mpBean : ManagementFactory.getMemoryPoolMXBeans()) {
//            if (mpBean.getType() == MemoryType.HEAP) {
//                System.out.printf(
//                        "Name: %s: %s\n",
//                        mpBean.getName(), mpBean.getUsage()
//                );
//            }
//        }

//        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
//
//        for(Long threadID : threadMXBean.getAllThreadIds()) {
//            ThreadInfo info = threadMXBean.getThreadInfo(threadID);
//            System.out.println("Thread name: " + info.getThreadName());
//            System.out.println("Thread State: " + info.getThreadState());
//            System.out.println(String.format("CPU time: %s ns",
//                    threadMXBean.getThreadCpuTime(threadID)));
//        }

        //     Scanner sc = new Scanner(System.in);
//     while(sc.hasNext()) {
//         scores.add(sc.nextInt());
//
//
//         Runtime runtime = Runtime.getRuntime();
//
//         System.out.println("Used Memory:"
//                 + (runtime.totalMemory() - runtime.freeMemory()));
//
//         System.out.println("Free Memory:"
//                 + runtime.freeMemory());
//
//         System.out.println("Total Memory:" + runtime.totalMemory());
//
//         System.out.println("Max Memory:" + runtime.maxMemory());
//     }


//        // Get the Java runtime
//        Runtime runtime = Runtime.getRuntime();
//// Run the garbage collector
//        runtime.gc();
//// Calculate the used memory
//        long memory = runtime.totalMemory() - runtime.freeMemory();
//        System.out.println("Used memory is bytes: " + memory);
//        System.out.println("free memory is bytes: " + runtime.freeMemory());
//        List<Model> list = new ArrayList<Model>();
//        for (int i = 0; i <= 10000000; i++) {
//            list.add(new Model("Jim", "Knopf"));
//        }
//        // Get the Java runtime
//        Runtime runtime2 = Runtime.getRuntime();
//        // Run the garbage collector
//        runtime2.gc();
//        // Calculate the used memory
//        long memory2 = runtime2.totalMemory() - runtime2.freeMemory();
//        System.out.println("Used memory is bytes: " + memory2);
//        System.out.println("free memory is bytes: " + runtime2.freeMemory());
//
    }
}