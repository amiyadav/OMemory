package org.amity.storage;

import org.amity.helper.Bits;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * first 4 short values are for active partition
 * 5 th position for NEXT_AVAILABLE_PARTITION_INDEX
 * <p>
 * TODO : Remaining left for saving other metadata in future
 */
public class MetadataInfo implements AutoCloseable {
    public static final int NEXT_AVAILABLE_PARTITION_INDEX = 5;
    public static final int READABLE_STARTING_POSITION = 16 * 10;
    public static final int NEXT_AV_PART_POSITION = 16 * 4;
    private final RandomAccessFile file;
    public final short[] activePartitions = new short[4];
    public static AtomicInteger totalPartitionCounter = new AtomicInteger();
    public AtomicInteger nextAvailablePartition;
    private FileChannel fileChannel;

    public MetadataInfo(String metaFileName) {
        try {
            this.file = new RandomAccessFile(metaFileName, "rw");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    // update activePartition array too
    public void updateActiveWritableAndReadablePartition(short oldPartitionValue, short newPartitionValue,
                                                         short nextAvailablePartition) throws IOException {
        replaceActivePartitionOnMetaFile(oldPartitionValue, newPartitionValue, nextAvailablePartition);

        IntStream.range(0, StorageManager.MAX_WRITE_THREAD_ALLOWED).forEach(i -> {
            if (activePartitions[i] == oldPartitionValue) {
                activePartitions[i] = newPartitionValue;
            }
        });
        // TODO check if we can move this anywhere
//        nextAvPartition.incrementAndGet();
    }

    void replaceActivePartitionOnMetaFile(short oldValue, short newValue, short nextAvailablePartition) throws IOException {
        ByteBuffer b = ByteBuffer.wrap(new byte[StorageManagerImpl.PAGE_SIZE]);
        this.fileChannel.read(b, StorageManager.metaPageOffset());
        // skip short byte for
        b.position(0);
        // Range
        IntStream.range(0, NEXT_AVAILABLE_PARTITION_INDEX).filter(i -> {
            // oldValue == Short.MIN_VALUE && value == newValue
            short value = (short) (b.getShort() & 0xFFFF);
            // mark the current position
            b.mark();
            if (oldValue == value) {
                // reset to mark position
                // TODO check carefully
                b.reset();
                // replace old value
                b.putShort((short) (newValue & 0xFFFF));

                b.rewind();
                // point to the position where entry of partition needs to be done
                b.position(NEXT_AV_PART_POSITION);
                b.putShort(nextAvailablePartition);
                try {
                    // position is 0 for master page
                    b.rewind();
                    this.fileChannel.write(b, StorageManager.metaPageOffset());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return true;
            }
            return false;
        }).findFirst();
        // Update bit so thread is readable
        updateBitToMakePartitionReadable(b);
    }

    /**
     * @throws IOException
     */
    private void commitPartitionOffset(short position) throws IOException {
        ByteBuffer b = ByteBuffer.wrap(new byte[StorageManagerImpl.PAGE_SIZE]);
        this.fileChannel.read(b, StorageManager.metaPageOffset());
        // skip short byte for
        b.position(0);
        // make bit ZERO to ONE
        updateBitToMakePartitionWritable(b, position);
    }

    /**
     * make first ZERO bit to ONE --> making readable
     *
     * @param b
     */
    private void updateBitToMakePartitionReadable(ByteBuffer b) {
        // update the readable bit true
        int position = Bits.getNextZeroBitFromPosition(READABLE_STARTING_POSITION, b.array());
        if (position == Short.MIN_VALUE) {
            System.out.println(" No partition to read ");
        }
        if (position > StorageManager.MAX_PARTITION_ALLOWED) {
            throw new IndexOutOfBoundsException(" position of readable page bit is out of bound");
        }

        Bits.setBit(b.array(), position, Bits.Bit.ONE);
        b.rewind();
        try {
            this.fileChannel.write(b, StorageManager.metaPageOffset());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * make first ONE bit to ZERO --> making writable again to recover
     *
     * @param b
     * @param position
     */
    public void updateBitToMakePartitionWritable(ByteBuffer b, short position) {
        // update the readable bit true
//        short pos = Bits.getNextOneBitFromPosition(READABLE_STARTING_POSITION, b.array());
        if (Bits.getBit(b.array(), position) == Bits.Bit.ZERO) {
            System.out.println(" Partition is already in writable mode i.e. o");
        }
        if (position == Short.MIN_VALUE) {
            //
            System.out.println(" No partition to read ");
        }
        if (position > StorageManager.MAX_PARTITION_ALLOWED) {
            throw new IndexOutOfBoundsException(" position of readable page bit is out of bound");
        }

        Bits.setBit(b.array(), position, Bits.Bit.ZERO);
        b.rewind();
        try {
            this.fileChannel.write(b, StorageManager.metaPageOffset());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    void init() {
        assert (this.fileChannel == null);
        try {
            this.fileChannel = this.file.getChannel();
            long fileLength = this.file.length();
            if (fileLength == 0) {
                // update active partition counter
                // update next available counter (there might be as where we can't evaluate next based on active partion counter
                // if any failed in-between 1, 997, 998, 999)
                // write master page to file
                this.writeMetaPageForPartitionAtInit();
            } else {
                // load master and header pages
                ByteBuffer b = ByteBuffer.wrap(new byte[StorageManager.PAGE_SIZE]);
                // read master page in bytebuffer b
                this.fileChannel.read(b, StorageManager.metaPageOffset());
                // reset position to zero to read each header 16 bit info
                b.position(0);
                IntStream.range(0, 5).forEach(i -> {
                    short value = (short) (b.getShort() & 0xFFFF);
                    // 5 th position is next available partition index
                    if (i == NEXT_AVAILABLE_PARTITION_INDEX) {
                        nextAvailablePartition = new AtomicInteger(value);
                    } else {
                        // read each header information and populate masterPage array
                        this.activePartitions[i] = value;
                    }
                });

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeMetaPageForPartitionAtInit() {
        /**
         * This is for metadata page
         */
        ByteBuffer buffer = ByteBuffer.wrap(new byte[StorageManagerImpl.PAGE_SIZE]);
        IntStream.range(0, 5).forEach(i -> {
            short value = (short) (i & 0xFFFF);
            if (i == NEXT_AVAILABLE_PARTITION_INDEX) {
                nextAvailablePartition = new AtomicInteger(value + 1);
            } else {
                activePartitions[i] = value;
            }
            buffer.putShort((short) (i & 0xFFFF));
        });
        // position is 0 for master page
        buffer.position(0);

        try {
            fileChannel.write(buffer, StorageManager.metaPageOffset());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public short[] getActivePartitions() {
        return activePartitions;
    }


    @Override
    public void close() throws Exception {

    }
}