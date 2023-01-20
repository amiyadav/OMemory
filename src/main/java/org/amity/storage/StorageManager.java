package org.amity.storage;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public interface StorageManager extends AutoCloseable {
    short PAGE_SIZE = 4096; // size of a page in bytes
    long INVALID_PAGE_NUM = -1L; // a page number that is always invalid
    // 1 pages per partition
    int MAX_HEADER_PAGES = 1;
    int MAX_READER_HEADER_PAGES = 1;
    // consider 2 bit per data
    int DATA_PAGES_PER_HEADER = (PAGE_SIZE / 2) * 8; // 32 k bits with 2 bit for each data page means 16K pages
    int MAX_WRITE_THREAD_ALLOWED = 4;
    // 16 gb for 128 partition
    int MAX_PARTITION_ALLOWED = 128; // 1 GB = 8 partition of 128 MB each
    // TODO CONFIGURE USING EXTERNAL CONFIGURATION OR CALCULATE OPTIMIZED THREAD NUMBER
    // 4 threads means 4 partition and 8 header pages
    int DEFAULT_OPTIMIZED_WRITE_THREADS = 4;
    // 4 threads means 4 partition and 8 header pages
    int DEFAULT_OPTIMIZED_READ_THREADS = 2;
    // default partition size in MB
    int DEFAULT_PARTITION_SIZE = 128;
    // default db file size in GB
    int DEFAULT_FILE_SIZE = 128;

    long allocPage(int partNum) throws IOException;

    void allocPart();

    void allocPart(int oldPartNum, int newPartNum) throws IOException;

    /**
     * Allocate new partition its like a topic
     *
     * @return
     */
    int assignNewPartitionAndUpdateMeta(int newPartNum, int partNum) throws IOException;


    /**
     * Get the partition number
     *
     * @param page
     * @return
     */
    static int getPartNum(long page) {
        return (int) (page / 10000000000L);
    }


    /**
     * Get Page number
     *
     * @param page
     * @return
     */
    static int getPageNum(long page) {
        return (int) (page % 10000000000L);
    }

    /**
     * Read gives page number in buffer
     *
     * @param page
     * @param buf
     */
    void readPage(long page, byte[] buf);

    /**
     * Write buf to page at given adrs
     *
     * @param page
     * @param buf
     */
    void writePage(long page, byte[] buf) throws IOException;

    /**
     * Commit the offset and make it available for write
     *
     * @param page
     */
    void freePage(long page);

    /**
     * Read gives page number in buffer
     *
     * @param pages
     * @param bufs
     */
    void readPages(List<Long> pages, List<byte[]> bufs);

    /**
     * Write buf to page at given address containing partition, header and page number
     *
     * @param pages
     * @param bufs
     */
    void writePages(List<Long> pages, List<byte[]> bufs);

    /**
     * Get virtual page number
     *
     * @param partNum
     * @param pageNum
     * @return
     */
    static long getVirtualPageNum(int partNum, int pageNum) {
        return partNum * 10000000000L + pageNum;
    }

    /**
     * @return offset in OS file for master page
     */
    static long masterPageOffset() {
        return 0;
    }

    /**
     * @return offset in OS file for master page
     */
    static long metaPageOffset() {
        return 0;
    }

    @Override
    void close();

}
