package org.amity.storage;

import org.amity.helper.Bits;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.OptionalInt;
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
 * and write the page and then update the WHP bit and Master Page and RHP
 * 1.2 If space not available it will reassign itself to a new partition
 * 1.2.1 if partition available then do 1.1
 * 1.2.2 else throw exception either all the partition are full or the reader is blocked/slowed down due
 * to client down/ system resource unavailability (go check metrics)
 * 2. Reader thread will check if any page is available to read i.e. 1
 * 2.1 If not then it will retry after configured time
 * 2.2 else read the page and send to the connector/client and update the offset i.e turned the same bit to 0
 */
public class PartitionInfo implements AutoCloseable {
    public static final int DATA_PAGE_BITS_HEADER = 1;
    private int partitionNumber;
    private short masterPage;
    private short readerMasterPage;
    private RandomAccessFile fileWriter;
    private FileChannel fileChannelWriter;
    private RandomAccessFile fileReader;
    private FileChannel fileChannelReader;
    private MetadataInfo metadataInfo;
    /**
     * This is configurable writer
     */
    private List<byte[]> writerHeaderPages;
    /**
     * This is configurable reader
     */
    private List<byte[]> readerHeaderPages;

    public PartitionInfo(int partitionNumber, MetadataInfo metadataInfo) {
        this.partitionNumber = partitionNumber;
        // byte[] 64K bit 10101010101 -> here we took pair for one data page 10 -> 1 is page is writable not readable
        this.writerHeaderPages = new ArrayList<>();
        this.readerHeaderPages = new ArrayList<>();
//        this.fileReader = fileReader;
//        this.fileChannelReader = fileChannelReader;
//        this.fileWriter = fileWriter;
//        this.fileChannelWriter = fileChannelWriter;
        readerHeaderPages = new ArrayList<>();
        writerHeaderPages = new ArrayList<>();
        this.metadataInfo = metadataInfo;
    }


    @Override
    public void close() throws Exception {

    }

    /**
     * Populate master and header pages for both Reader and Writer pages
     * 1. Writer : M.p -> H.p -> D.p
     * 2. Reader : M.p -> H.p (enough to maintain state of 32k pages)
     *
     * @param partFilePath Reader or write file paths
     * @param newPartNum   new Partition to allocate
     */
    public void allocateFileForPartition(String partFilePath, int newPartNum) {
        assert (this.fileChannelWriter == null);
        try {
            // Init file reader channel
            this.fileReader = new RandomAccessFile(partFilePath + newPartNum + "/prt" + newPartNum, "rw");
            this.fileChannelReader = this.fileReader.getChannel();
            //Init file writer channel
            this.fileWriter = new RandomAccessFile(partFilePath + newPartNum + "/offset" + newPartNum, "rw");
            this.fileChannelWriter = this.fileWriter.getChannel();

            long fileLength = this.fileWriter.length();
            if (fileLength == 0) {
                IntStream.range(0, StorageManager.MAX_HEADER_PAGES).forEachOrdered(x -> {
                    // means new file write empty master page and fill header page with null
                    this.readerHeaderPages.add(null);
                    this.writerHeaderPages.add(null);
                });
                // init writer master page to max value of short
                this.writeOrReadMasterPageForPartition(masterPage, true);

                // init reader master page to max value of short
                this.writeOrReadMasterPageForPartition(readerMasterPage, false);
            } else {

                // load writer master page and headers
                loadExistingMasterPage(fileLength, true);
                // load reader master page and headers
                loadExistingMasterPage(fileLength, false);
            }

        } catch (Exception ex) {
            throw new PageException("Could not open or read file: " + partFilePath + " EX :" + ex.getMessage());
        }
    }

    private void loadExistingPage(List<byte[]> headerPages,
                                  FileChannel fileChannel,
                                  long position) {
        // load reader header page
        byte[] headerPage = new byte[StorageManager.PAGE_SIZE];
        headerPages.add(headerPage);
        try {
            fileChannel.read(ByteBuffer.wrap(headerPage), position);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void loadExistingMasterPage(long fileLength, boolean isWriter) throws IOException {
        // load master and header pages
        ByteBuffer b = ByteBuffer.wrap(new byte[StorageManager.PAGE_SIZE]);
        if (isWriter) {
            // read master page in bytebuffer b
            this.fileChannelWriter.read(b, StorageManager.masterPageOffset());
            // reset position to zero to read each header 16 bit info
            b.position(0);
            // read each header information and populate masterPage array
            this.masterPage = (short) (b.getShort() & 0xFFFF);
            IntStream.range(0, StorageManager.MAX_HEADER_PAGES).forEach(i -> {
                // check if this header pages exist within file length range
                if (headerPageOffset(i) > fileLength) {
                    this.writerHeaderPages.add(null);
                } else {
                    // for each header page load from disk/file
                    loadExistingPage(this.writerHeaderPages, this.fileChannelWriter, headerPageOffset(i));
                }
            });
        } else {
            // read master page in bytebuffer b
            this.fileChannelWriter.read(b, StorageManager.masterPageOffset());
            // reset position to zero to read each header 16 bit info
            b.position(0);
            // read each header information and populate masterPage array
            this.readerMasterPage = (short) (b.getShort() & 0xFFFF);
            loadExistingPage(this.readerHeaderPages, this.fileChannelReader, readerHeaderPageOffset(0));
        }
    }


    // TODO Don't allow record having size > PAGE_SIZE
    // Also skip pre-filled pages
    // 4048 * 8 bit = 32 Kb
    // skip the whole page if bits started with 10 or 00
    // check pair of 01 01
    boolean checkActivePartition(short masterPage) {
        // skip if pre-filled / check if all 32k data pages are filled
        return masterPage != Short.MAX_VALUE;

    }

    /**
     * For 0 headerIndex its only master page to skip
     * for 1 -> masterPage + headerPage + dataPages
     *
     * @param headerIndex which header page
     * @return offset in OS file for header page
     */
    private static long headerPageOffset(int headerIndex) {
        // TODO check headerIndex
        return StorageManager.PAGE_SIZE +
                (headerIndex + (long) headerIndex * StorageManager.DATA_PAGES_PER_HEADER) * StorageManager.PAGE_SIZE;
    }

    /**
     * For 0 headerIndex its only master page and header page --> masterPage
     * for 1 -> masterPage + headerPage
     * for 2 -> masterPage + headerPage + headerPage
     *
     * @param headerIndex which header page
     * @return offset in OS file for header page
     */
    private static long readerHeaderPageOffset(int headerIndex) {
        // TODO check headerIndex
        return StorageManager.PAGE_SIZE + (long) headerIndex * StorageManager.PAGE_SIZE;
    }

    /**
     * This is either for master of writer or reader page
     */
    private void writeOrReadMasterPageForPartition(short page, boolean isWriter) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[StorageManager.PAGE_SIZE]);

        // insert 16 bit which represent
        // A 16-bit integer can store 2^16 (or 65,536) distinct values.
        // In an unsigned representation, these values are the integers
        // between 0 and 65,535; using two's complement, possible values
        // range from −32,768 to 32,767. Hence, a processor with 16-bit memory
        // addresses can directly access 64 KB of byte-addressable memory.
        // similarly for 8 bit we can have 2 header pages -> 128 MB

        // 16 bit can represent allocation of 65536 pages
        // and on 1 header page is holding references of 16384 Data pages
        // that means we can reference 4 header pages from 16 bit

        //0xFFFF is hexidecimal (base 16) meaning that the number is made up of
        //0,1,2,3,4,5,6,7,8,9,A,B,C,D,E,F where A=10, B=11, etc. I like to think of it this way:
        //16^3 (4096) 16^2 (256) 16^1 (16) 16^0 (1)
        //--------------------------------------------------
        //F F F F
        //
        //this means that 0xFFFF is 15*4096 + 15*256 + 15*16 + 15*1.
        //In base 10 this number would be represented like this:
        //10^4 (10000) 10^3 (1000) 10^2 (100) 10^1 (10) 10^0 (1)
        //--------------------------------------------------------------
        //6 5 5 3 5

        //Java allows us to define numbers interpreted as hex (base 16) by using the 0x prefix,
        //followed by an integer literal.
        //
        //The value 0xff is equivalent to 255 in unsigned decimal, -127 in signed decimal,
        //and 11111111 in binary.
        //
        //So, if we define an int variable with a value of 0xff, since Java represents integer
        //numbers using 32 bits, the value of 0xff is 255:
        // & -> 1 - 1 is 1 otherwise 0
        // https://www.javatpoint.com/java-and-0xff-example

        buffer.putShort((short) (page & 0xFFFF));
        // position is 0 for master page
        buffer.position(0);

        try {
            if (isWriter) {
                fileChannelWriter.write(buffer, StorageManager.masterPageOffset());
            } else {
                fileChannelReader.write(buffer, StorageManager.masterPageOffset());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Writes to a data page. Assumes that the partition lock is held.
     * Validate : if bit group is 1,0 (W, N-R) then only allow
     * <p>
     * Update
     *
     * @param pageNum data page number to write to
     * @param buf     input buffer with new contents of page - assumed to be page size
     */
    public void writePage(int pageNum, byte[] buf) throws IOException {
        if (isNotAllocatedPage(pageNum, true)) {
            throw new PageException("page " + pageNum + " is not allocated");
        }

        ByteBuffer buffer = ByteBuffer.wrap(buf);
        this.fileChannelWriter.write(buffer, PartitionInfo.getDataPageOffset(pageNum));
        this.fileChannelWriter.force(false);

        // TODO check if we want to maintain dirty page table for memory
        // I don't we need it as single thread is going to access this partition
//        long vpn = StorageManagerImpl.getVirtualPageNum(partNum, pageNum);
//        recoveryManager.diskIOHook(vpn);
    }


    /**
     * (2 (for master and header page)  * pagesize) + (100 * pagesize)
     * (this is for skipping 2nd header page - pageNum / StorageManagerImpl.DATA_PAGES_PER_HEADER)
     *
     * @param pageNum
     * @return
     */
    private static long getDataPageOffset(int pageNum) {
//        return (long) (2 + pageNum) * StorageManagerImpl.PAGE_SIZE;
        return (long) (2 + pageNum / StorageManagerImpl.DATA_PAGES_PER_HEADER + pageNum) * StorageManagerImpl.PAGE_SIZE;
    }

    private boolean isNotAllocatedPage(int pageNum, boolean isWriter) {
        // 32k data pages per header
        // pageNum < 32k have partition 1 for virtual pageNum 1000000064
        int headerIndex = pageNum / StorageManagerImpl.DATA_PAGES_PER_HEADER;
        // 1000000064 % 16k = 64 page or 64 th bit in header page
        int dataPageIndex = pageNum % StorageManagerImpl.DATA_PAGES_PER_HEADER;
        //
        if (headerIndex < 0 || headerIndex >= StorageManagerImpl.MAX_HEADER_PAGES) {
            return true;
        }

        //
        if (!isWriter && readerMasterPage == 0) {
            return true;
        } else if (isWriter && masterPage == 0) {
            return true;
        }

        return isWriter ? Bits.getBit(writerHeaderPages.get(headerIndex), dataPageIndex) == Bits.Bit.ZERO
                : Bits.getBit(readerHeaderPages.get(headerIndex), dataPageIndex) == Bits.Bit.ZERO;
    }

//    private boolean isReadablePage(int pageNum) {
//        // 16k data pages per header
//        // pageNum < 16k have partition 1 for virtual pageNum 1000000064
//        int headerIndex = pageNum / StorageManagerImpl.DATA_PAGES_PER_HEADER;
//        // 1000000064 % 16k = 64 page or 64*2 th bit in header page
//        int dataPageIndex = pageNum % StorageManagerImpl.DATA_PAGES_PER_HEADER;
//        //
//        if (headerIndex < 0 || headerIndex >= StorageManagerImpl.MAX_HEADER_PAGES) {
//            return true;
//        }
//        if (masterPage == 0) {
//            return true;
//        }
//
//        return Bits.getBit((headerIndex == 0 ? headerPages.getLeft() : headerPages.getRight()), dataPageIndex * DATA_PAGE_BITS_HEADER) == Bits.Bit.ZERO &&
//                Bits.getBit((headerIndex == 0 ? headerPages.getLeft() : headerPages.getRight()), dataPageIndex * DATA_PAGE_BITS_HEADER + 1) == Bits.Bit.ONE;
//    }

    public void readPage(int pageNum, byte[] buf) throws IOException {
        // check if partition is readed fully
        // if full then update partition metadata and mark this partition bit zero
        if (readerMasterPage == Short.MAX_VALUE) {
            ByteBuffer bb = ByteBuffer.wrap(buf);
            metadataInfo.updateBitToMakePartitionWritable(bb, (short) partitionNumber);
        }

        if (this.isNotAllocatedPage(pageNum, false)) {
            throw new PageException("page " + pageNum + " is not allocated");
        }

        ByteBuffer b = ByteBuffer.wrap(buf);
        this.fileChannelWriter.read(b, PartitionInfo.getDataPageOffset(pageNum));
    }

    // update header page : commit offset (reading is done) for page i.e. update reader page bit to 0
    // and update the master page
    public void commitOffsetForPage(int pageNum) throws IOException {
        int headerIndex = pageNum / StorageManagerImpl.DATA_PAGES_PER_HEADER;
        int pageIndex = pageNum % StorageManagerImpl.DATA_PAGES_PER_HEADER;

        byte[] headerBytes = readerHeaderPages.get(StorageManager.MAX_HEADER_PAGES - 1);
        if (headerBytes == null) {
            throw new NoSuchElementException("header page : " + headerIndex + " is empty");
        }

        Bits.Bit bit = Bits.getBit(headerBytes, pageIndex * DATA_PAGE_BITS_HEADER);
        if (bit == Bits.Bit.ZERO) {
            // TODO warn
            throw new PageException("already commit offset for this page with : " + bit);
        }

        // mark readable byte as zero
        Bits.setBit(headerBytes, pageIndex * DATA_PAGE_BITS_HEADER + 1, Bits.Bit.ZERO);
        // update master page with counting WR or 10 pages
        // this.masterPage = Bits.countOnMode(headerBytes, Bits.Mode.WR);

        // Number of 1's bit in byte array
        this.masterPage = (short) Bits.countBits(headerBytes);

        this.writeOrReadMasterPageForPartition(this.masterPage, false);
        this.writeHeaderPage(headerIndex);
    }

    /**
     * Writes a header page to disk.
     *
     * @param headerIndex which header page
     */
    private void writeHeaderPage(int headerIndex) throws IOException {
        // TODO check master pages count
        byte[] headerBytes = writerHeaderPages.get(0);
        ByteBuffer b = ByteBuffer.wrap(headerBytes);
        this.fileChannelWriter.write(b, PartitionInfo.headerPageOffset(headerIndex));
    }

    /**
     * Writes a header page to disk.
     *
     * @param headerIndex which header page
     */
    private void writeReaderPage(int headerIndex) throws IOException {
        byte[] headerBytes = readerHeaderPages.get(headerIndex);
        ByteBuffer b = ByteBuffer.wrap(headerBytes);
        this.fileChannelReader.write(b, PartitionInfo.readerHeaderPageOffset(headerIndex));
    }

    /**
     * Check if Page is available
     *
     * @return
     */
    public int allocPage() throws IOException {
        int headerIndex = -1;
        for (int i = 0; i < StorageManager.MAX_HEADER_PAGES; ++i) {
            // check if page is available
            if (this.masterPage < StorageManager.DATA_PAGES_PER_HEADER) {
                headerIndex = i;
                break;
            }
        }
        if (headerIndex == -1) {
            // return some max int indicating allocate new partition
            return Integer.MIN_VALUE;
//            throw new PageException("no free pages - partition has reached max size");
        }

        // get the header bytes
        byte[] headerBytes = this.writerHeaderPages.get(headerIndex);

        OptionalInt pageIndex;
        // if initial page available
        if (headerBytes == null) {
            pageIndex = OptionalInt.of(0);
        } else {
            // pageIndex which is available 11110 -> 4th index available
            pageIndex = IntStream.range(0, StorageManager.DATA_PAGES_PER_HEADER)
                    .filter(position -> Bits.getBit(headerBytes, position) == Bits.Bit.ZERO)
                    .findFirst();
        }
        if (pageIndex.isEmpty()) {
            throw new PageException("header page should have free space, but doesn't");
        }
        return this.allocPage(headerIndex, pageIndex.getAsInt());
    }

    /**
     * Check if Page is available
     *
     * @return
     */
    public int allocReaderPage() throws IOException {
        byte[] headerBytes;
        OptionalInt pageIndex = OptionalInt.empty();
        for (int headerIndex = 0; headerIndex < StorageManager.MAX_READER_HEADER_PAGES; headerIndex++) {
            // get the header bytes
            headerBytes = this.readerHeaderPages.get(headerIndex);

            // if initial page available
            if (headerBytes == null) {
                pageIndex = OptionalInt.of(0);
            } else {
                // pageIndex which is available 11110 -> 4th index available
                for (int i = 0; i < StorageManager.DATA_PAGES_PER_HEADER; i++) {
                    if (Bits.getBit(headerBytes, i) == Bits.Bit.ZERO) {
                        pageIndex = OptionalInt.of(i);
                        break;
                    }
                }
            }
            if (pageIndex.isEmpty()) {
                throw new PageException("header page should have free space, but doesn't");
            }


            if (headerBytes == null) {
                headerBytes = new byte[StorageManager.PAGE_SIZE];
                this.readerHeaderPages.remove(headerIndex);
                this.readerHeaderPages.add(headerIndex, headerBytes);
            }

            // check if already allocated
            if (Bits.getBit(headerBytes, pageIndex.getAsInt()) == Bits.Bit.ONE) {
                throw new IllegalStateException("page at (part=" + partitionNumber + ", header=" + headerIndex + ", index="
                        +
                        pageIndex + ") already allocated");
            }

            this.readerMasterPage = (short) Bits.countBits(headerBytes);
            // update count of existing
            this.writeOrReadMasterPageForPartition((short) pageIndex.getAsInt(), false);
            // write down updated header page
            this.writeReaderPage(headerIndex);
        }
        return pageIndex.getAsInt();
    }

    /**
     * Allocate a new page in partition
     *
     * @param headerIndex
     * @param pageIndex
     * @return
     */
    private int allocPage(int headerIndex, int pageIndex) throws IOException {
        byte[] headerBytes = this.writerHeaderPages.get(headerIndex);
        if (headerBytes == null) {
            headerBytes = new byte[StorageManager.PAGE_SIZE];
            this.writerHeaderPages.remove(headerIndex);
            this.writerHeaderPages.add(headerIndex, headerBytes);
        }

        // check if already allocated
        if (Bits.getBit(headerBytes, pageIndex) == Bits.Bit.ONE) {
            throw new IllegalStateException("page at (part=" + partitionNumber + ", header=" + headerIndex + ", index="
                    +
                    pageIndex + ") already allocated");
        }

        Bits.setBit(headerBytes, pageIndex, Bits.Bit.ONE);
        this.masterPage = (short) Bits.countBits(headerBytes);

        int pageNum = pageIndex + headerIndex * StorageManager.DATA_PAGES_PER_HEADER;

        // write down updated master page
        this.writeOrReadMasterPageForPartition(this.masterPage, true);
        // write down updated header page
        this.writeHeaderPage(headerIndex);

        return pageNum;
    }

    /**
     * Update the reader master page count and header page bit
     *
     * @param pageNum
     * @throws IOException
     */
    public void updateReaderPageBit(int pageNum) throws IOException {
        // update read bit
        int headerIndex = pageNum / StorageManagerImpl.DATA_PAGES_PER_HEADER;
        int pageIndex = pageNum % StorageManagerImpl.DATA_PAGES_PER_HEADER;

        byte[] headerBytes = readerHeaderPages.get(0);
        if (headerBytes == null) {
            throw new NoSuchElementException("Reader page is empty and unallocated");
        }

        // return if already in read mode
        Bits.Bit bit = Bits.getBit(readerHeaderPages.get(0), pageIndex);
        if (bit == Bits.Bit.ONE) {
            return;
        }
        // mark readable byte as ONE
        Bits.setBit(headerBytes, pageIndex, Bits.Bit.ONE);
        // update master page
        this.readerMasterPage = (short) Bits.countBits(headerBytes);

        this.writeOrReadMasterPageForPartition(this.readerMasterPage, false);
        this.writeReaderPage(headerIndex);
        // update the metadata if needed also
    }
}