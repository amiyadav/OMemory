package org.amity.helper;

import java.util.BitSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class Bits {


    public enum Bit {ZERO, ONE}

    /**
     * Counts the number of set bits. For example:
     * <p>
     * - countBits(0b00001010) == 2
     * - countBits(0b11111101) == 7
     */
    public static int countBits(byte b) {
        return Integer.bitCount(b);
    }

    /**
     * Counts the number of set bits.
     */
    public static int countBits(byte[] bytes) {
        int count = 0;
        for (byte b : bytes) {
            count += countBits(b);
        }
        return count;
    }


    /**
     * Set the ith bit of a byte array where the 0th bit is the most significant
     * bit of the first byte (arr[0]). An example:
     * <p>
     * byte[] buf = new bytes[2]; // [0b00000000, 0b00000000]
     * setBit(buf, 0, ONE); // [0b10000000, 0b00000000]
     * setBit(buf, 1, ONE); // [0b11000000, 0b00000000]
     * setBit(buf, 2, ONE); // [0b11100000, 0b00000000]
     * setBit(buf, 15, ONE); // [0b11100000, 0b00000001]
     */
    public static void setBit(byte[] bytes, int i, Bit bit) {
        bytes[i / 8] = setBit(bytes[i / 8], i % 8, bit);
    }

    /**
     * Set the ith bit of a byte where the 0th bit is the most significant bit
     * and the 7th bit is the least significant bit. Some examples:
     * <p>
     * - setBit(0b00000000, 0, ONE) == 0b10000000
     * - setBit(0b00000000, 1, ONE) == 0b01000000
     * - setBit(0b00000000, 2, ONE) == 0b00100000
     */
    static byte setBit(byte b, int i, Bit bit) {
        if (i < 0 || i >= 8) {
            throw new IllegalArgumentException(String.format("index %d out of bounds", i));
        }
        byte mask = (byte) (1 << (7 - i));
        switch (bit) {
            case ZERO: {
                return (byte) (b & ~mask);
            }
            case ONE: {
                return (byte) (b | mask);
            }
            default: {
                throw new IllegalArgumentException("Unreachable code.");
            }
        }
    }

    /**
     * Get the ith bit of a byte array where the 0th bit is the most significant
     * bit of the first byte. Some examples:
     * <p>
     * - getBit(new byte[]{0b10000000, 0b00000000}, 0) == ONE
     * - getBit(new byte[]{0b01000000, 0b00000000}, 1) == ONE
     * - getBit(new byte[]{0b00000000, 0b00000001}, 15) == ONE
     */
    public static Bit getBit(byte[] bytes, int x) {
        if (bytes.length == 0 || x < 0 || x >= bytes.length * 8) {
            String err = String.format("bytes.length = %d; i = %d.", bytes.length, x);
            throw new IllegalArgumentException(err);
        }

        // x = 800 -> x/8 = 100 (100 byte in header page) and 800 % 8 = 0th bit
        return getBit(bytes[x / 8], x % 8);
    }

    /**
     * Get the ith bit of a byte where the 0th bit is the most significant bit
     * and the 7th bit is the least significant bit. Some examples:
     * <p>
     * - getBit(0b10000000, 7) == ZERO
     * - getBit(0b10000000, 0) == ONE
     * - getBit(0b01000000, 1) == ONE
     * - getBit(0b00100000, 1) == ZERO
     */
    static Bit getBit(byte b, int i) {
        if (i < 0 || i >= 8) {
            throw new IllegalArgumentException(String.format("index %d out of bounds", i));
        }
        return ((b >> (7 - i)) & 1) == 0 ? Bit.ZERO : Bit.ONE;
    }

    /**
     * Reverse bit order of each byte of the array
     *
     * @param data the bytes array
     * @return the bytes array with bit order reversed for each byte
     */
    private static byte[] reverse(byte[] data) {
        byte[] bytes = data.clone();

        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (Integer.reverse(bytes[i]) >>> 24);
        }

        return bytes;
    }

    public enum Mode {
        WNR, NWR, WR, NWNR;
    }

    /**
     * Count occurences of Mode
     *
     * @param bytes
     * @param mode
     * @return
     */
    public static short countOnMode(byte[] bytes, Bits.Mode mode) {
        BitSet bs = BitSet.valueOf(reverse(bytes));
        System.out.println(bs.length());
        System.out.println(bs.size());
        // Init the BitSet with an array of reversed bytes to handle the little endian representation expected
        short count = 0;
        for (int i = 0; i < (bytes.length * 8); i = i + 2) {
            switch (mode) {
                case WR:
                    // 11
                    if (bs.get(i) && bs.get(i + 1)) {
                        count++;
                    }
                    break;
                case WNR:
                    // 10 init state
                    // true - false
                    if (bs.get(i) && !bs.get(i + 1)) {
                        count++;
                    }
                    break;
                case NWR:
                    // 01
                    if (!bs.get(i) && bs.get(i + 1)) {
                        count++;
                    }
                    break;
                case NWNR:
                    // 00
                    if (!bs.get(i) && !bs.get(i + 1)) {
                        count++;
                    }
                    break;
                default:
                    throw new NoSuchElementException(" Invalid Mode !!!");
            }
        }
        return count;
    }

    /**
     * skip first 5 shorts (5 * 16 byte or 5 * 16*8 bite)
     * 5 * 128 = 640bits...(each partition)11110000
     */
    public static int getNextZeroBitFromPosition(int position, byte[] bytes) {
        BitsIterator iterator = new BitsIterator(bytes);
        Iterator<Boolean> itr = iterator.iterator();
        int count = 0;
        while (itr.hasNext()) {
            if (count < position) {
                count++;
                continue;
            }
            // find next available partition
            if (!itr.next()) {
                return count;
            } else {
                count++;
            }

        }
        return Short.MIN_VALUE;
    }

    public static short getNextOneBitFromPosition(int position, byte[] bytes) {
        BitsIterator iterator = new BitsIterator(bytes);
        Iterator<Boolean> itr = iterator.iterator();
        short count = 0;
        while (itr.hasNext()) {
            if (count < position) {
                count++;
                continue;
            }
            // find next available partition
            if (itr.next()) {
                return count;
            } else {
                count++;
            }

        }
        return Short.MIN_VALUE;
    }

}
