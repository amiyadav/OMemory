package org.amity.storage;

import org.amity.helper.FileAndDirHelper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * Partition will contain
 * partition file (consist Master Page, WriterHeaderPage, Data Pages)
 * and reader file like an index for reader thread
 * <p>
 * Partition File : 4K M.P -> 4K H.P (Configurable) (32K bits) -> Data Pages 32k pages of 4k each
 * All Zero bits initially
 * Reader File : 4k page consist 32k bits of zeros 0.... initially
 * <p>
 * Writer Algo :
 * Each Writer and Reader works on their own partition i.e. if we have 8 threads then
 * 4 pair of reader/writer thread will be reading/writing its own partition among 4 partition
 * <p>
 * 1. Writer thread will check if space is available in partition from master page
 * 1.1 If space is available find the available page from WHP (i.e. first 0th bit) and create the buffer of 4k bytes
 * and write the page and then update the WHP (Write Header Page) bit and Master Page and RHP
 * 1.2 If space not available it will reassign itself to a new partition
 * 1.2.1 if partition available then do 1.1 and also do 3.1.1
 * 1.2.2 else throw exception either all the partition are full or the reader is blocked/slowed down due
 * to client down/ system resource unavailability (go check metrics)
 * <p>
 * Reader Algo :
 * configured number of reader threads
 * 2. Reader thread will check if any page is available to read i.e. 1
 * 2.1 If not then it will retry after configured time
 * 2.2 else read the page and send to the connector/client and update the offset i.e turned the same bit to 0
 * <p>
 * Meta Algo :
 * Meta information about the partition needs to be saved in meta file that is external to all the partition but in db dir
 * This file will contain information like
 * - Active Partition (short)
 * - NextAvailablePartition (short)
 * - Total partition (short)
 * - Partition to recover (short)
 * 3. First four short value are active partition number and next short value is NextAvailablePartition
 * and then next short value is total partition and then short numbers for each
 * recovery partition i.e. partition which already committed by reader thread or already read completely
 * 3.1 Configured Active partition needed to be updated at Omemory load time with or last configuration
 * 3.1.1 Also active partition need to be updated whenever writer thread switching to other partition
 * and update Partition to recover (short)
 */
public class StorageManagerImpl implements StorageManager {

    //    public AtomicInteger partitionCounter;
    public static String dbDir;
    public Map<Integer, PartitionInfo> partitionInfo;
    private MetadataInfo metadataInfo;


    int writeThreads;
    int readThreads;

    public StorageManagerImpl(String dbDir, int writeThreads, int readThreads) throws IOException {
        StorageManagerImpl.dbDir = dbDir == null ? System.getProperty("user.home") : dbDir;
        this.writeThreads = writeThreads == 0 ? DEFAULT_OPTIMIZED_WRITE_THREADS : writeThreads;
        this.readThreads = readThreads == 0 ? DEFAULT_OPTIMIZED_READ_THREADS : readThreads;
        // This will create new meta file or initialize meta file
        loadMetadata();
        // It has nothing to do with
        allocPart();

    }

    private void allocReaderPage(int partNum) throws IOException {
        PartitionInfo pi = partitionInfo.get(partNum);
        pi.allocReaderPage();
    }


    /**
     * Allocate partition equivalent to the max threads allowed to write
     */
    @Override
    public void allocPart() {
        IntStream.range(0, MAX_WRITE_THREAD_ALLOWED)
                .forEachOrdered(partitionNum -> {
//                    int oldPart = metadataInfo.nextAvailablePartition.get();
//                    int newPart = metadataInfo.nextAvailablePartition.updateAndGet((int x) -> Math.max(x, oldPart) + 1);
                    try {
                        this.allocPart(Short.MIN_VALUE, partitionNum);
                        System.out.println(" Partition allocated --> " + partitionNum);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /**
     * Note: metadata already loaded from disk
     * <p>
     * Allocate new partition with new partition number
     *
     * @param oldPartNum old partition number
     * @param newPartNum new partition number
     */
    @Override
    public void allocPart(int oldPartNum, int newPartNum) throws IOException {
//        metadataInfo.nextAvailablePartition.updateAndGet((int x) -> Math.max(x, newPartNum) + 1);
        int partNum = this.allocSinglePartition(newPartNum);
        System.out.println("allocated : " + partNum);
        // TODO handle failure scenario
        // update the meta information about the partition
        assignNewPartitionAndUpdateMeta(oldPartNum, newPartNum);
    }

    /**
     * create 800 partition or 1 GB where each partition is 64 MB
     * 1 Master page -> of size 16 (65536) with 2 Header Pages each having
     * 32K data pages
     * Each partition will have its own master page
     */
    public int allocSinglePartition(int newPartNum) throws IOException {
        // M.P --> H.P --> D.Pgs
        // Allocate all the partition
        if (this.partitionInfo.containsKey(newPartNum)) {
            throw new IllegalStateException("partition number " + newPartNum + " already exists");
        }
        PartitionInfo pInfo = new PartitionInfo(newPartNum, metadataInfo);
        this.partitionInfo.put(newPartNum, pInfo);

        // this will on load writer  and reader
        pInfo.allocateFileForPartition(FileAndDirHelper.PARTITION_PATH, newPartNum);

        // allocate reader page in partition
        pInfo.allocReaderPage();

        // Update the meta information and if already exists then do nothing or else
        if (MAX_PARTITION_ALLOWED != MetadataInfo.totalPartitionCounter.get()) {
            throw new PageException(" Incorrect # of partition allocated"
                    + MAX_PARTITION_ALLOWED + " --> " + MetadataInfo.totalPartitionCounter.get());
        }
        return newPartNum;
    }


    private void loadMetadata() {
        // create a file /db/meta
        metadataInfo = new MetadataInfo(FileAndDirHelper.DEFAULT_DB_PATH + "/" + "meta");
        // create new meta file or load existing meta information
        metadataInfo.init();
    }


    /**
     * Metadeta information
     * <p>
     * Commit offset of
     *
     * @param oldPartNum Old filled partition
     * @param newPartNum new partition to assigned
     * @return new partition number
     */
    @Override
    public int assignNewPartitionAndUpdateMeta(int oldPartNum, int newPartNum) throws IOException {
        OptionalInt existingPart = IntStream.range(0, metadataInfo.activePartitions.length)
                .filter(activePartition -> activePartition == newPartNum).findFirst();
        // this is a case when DB restarts
        if (existingPart.isPresent()) {
            // TODO : change to warn message
            System.out.println("Partition is already active " + newPartNum);
//            throw new DuplicateRequestException("Partition is already active " + newPartNum);
        } else {
            // when DB loaded first time or thread asking for new partition
            metadataInfo.updateActiveWritableAndReadablePartition((short) oldPartNum, (short) newPartNum, (short) (newPartNum + 1));
            metadataInfo.nextAvailablePartition.updateAndGet((int x) -> Math.max(x, newPartNum) + 1);
        }

        return newPartNum;
    }


    @Override
    public void readPage(long page, byte[] buf) {
        if (buf.length != PAGE_SIZE) {
            throw new IllegalArgumentException("writePage expects a page-sized buffer");
        }
        int partNum = StorageManager.getPartNum(page);
        int pageNum = StorageManager.getPageNum(page);
        PartitionInfo pi = getPartInfo(partNum);

        try {
            pi.readPage(pageNum, buf);
        } catch (IOException e) {
            throw new PageException("could not read partition " + partNum + ": " + e.getMessage());
        }
    }

    /**
     * Allocate new page in partition
     * 1. First find where to insert the page and then write bytebuffer
     *
     * @param page page to write
     * @param buf  byte array buffer
     */
    @Override
    public void writePage(long page, byte[] buf) throws IOException {
        if (buf.length != PAGE_SIZE) {
            throw new IllegalArgumentException("writePage expects a page-sized buffer");
        }
        int partNum = StorageManager.getPartNum(page);
        int pageNum = StorageManager.getPageNum(page);
        PartitionInfo pi = getPartInfo(partNum);

        try {
            pi.writePage(pageNum, buf);
        } catch (IOException e) {
            throw new PageException("could not write partition " + partNum + ": " + e.getMessage());
        }
        pi.updateReaderPageBit(pageNum);

    }

    /**
     * 1. Allocate the page and write down master
     * and header page
     * and if no pages are available then allocate new partition
     *
     * <p>
     * 2. Write down the page
     * Return virtual page number
     * 3. update the read Bit in reader header and also update reader master page count
     * 4. If partition is full then return Long.MIN_VALUE
     *
     * @param partNum partition number to be allocated
     * @return virtual page number
     */
    @Override
    public long allocPage(int partNum) throws IOException {
        // check if partition exists
        PartitionInfo pi = getPartInfo(partNum);
        // 1. allocate the page first
        // 2. update the master page
        // 3. update the header byte to disk
        int allocatedPage = pi.allocPage();

        if (Integer.MIN_VALUE == allocatedPage) {
            return Long.MIN_VALUE;
        }
        // write down the new page
        pi.writePage(partNum, new byte[PAGE_SIZE]);
        return StorageManager.getVirtualPageNum(partNum, allocatedPage);
    }

    @Override
    public void freePage(long page) {
        int partNum = StorageManager.getPartNum(page);
        int pageNum = StorageManager.getPageNum(page);
        PartitionInfo pi = getPartInfo(partNum);

        try {
            pi.commitOffsetForPage(pageNum);
        } catch (Exception e) {
            throw new PageException("could not write partition " + partNum + ": " + e.getMessage());
        }
    }

    private PartitionInfo getPartInfo(int partNum) {
        PartitionInfo pi = this.partitionInfo.get(partNum);
        if (pi == null) {
            throw new NoSuchElementException(" No Partition " + partNum);
        }
        return pi;
    }

    /**
     * @param pages pages to read
     * @param bufs  byte buffers
     */
    @Override
    public void readPages(List<Long> pages, List<byte[]> bufs) {
        if (pages.size() != bufs.size()) {
            throw new PageException("pages request to read " + pages.size() + ":: not equal to buffersize :  " + bufs.size());
        }
        AtomicInteger counter = new AtomicInteger(0);
        pages.forEach(page -> {
            readPage(page, bufs.get(counter.get()));
            counter.getAndIncrement();
        });
    }

    @Override
    public void writePages(List<Long> pages, List<byte[]> bufs) {
        if (pages.size() != bufs.size()) {
            throw new PageException("pages request to write " + pages.size() + ":: not equal to buffersize :  " + bufs.size());
        }
        AtomicInteger counter = new AtomicInteger(0);
        pages.forEach(page -> {
            try {
                writePage(page, bufs.get(counter.get()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            counter.getAndIncrement();
        });
    }

    @Override
    public void close() {

    }

}
