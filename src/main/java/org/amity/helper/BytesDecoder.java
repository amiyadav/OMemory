package org.amity.helper;

import java.util.Arrays;
import java.util.BitSet;
import java.util.NoSuchElementException;

/**
 *
 */
public class BytesDecoder {
    private BitSet bitSetToDecode;

    public static void main(String[] args) {
        // 0x12 -> 0b00010010 -> 18
        // 0x13 -> 0b00010011 -> 19
//        byte[] b = new byte[]{0b00010010, 0b00010011};
//        new BytesDecoder(b);

        byte[] b = new byte[]{0b01010010, 0b01101011};
        countOnMode(b, Mode.WNR);
    }

    private enum Mode {
        WNR, NWR, WR, NWNR;
    }

    public static short countOnMode(byte[] bytes, Mode mode) {
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
     * BytesDecoder constructor
     * Init the BitSet used for decoding
     *
     * @param bytes the byte array to decode
     */
    public BytesDecoder(byte[] bytes) {
        System.out.println("BytesDecoder ==== FRAME TO DECODE ====");
        System.out.println("BytesDecoder " + Arrays.toString(bytes));
        // Translate bytes array as binary
        StringBuilder frameBinary = new StringBuilder();
        for (byte b : bytes) {
            frameBinary.append(toBinary(b, 8));
        }
        // 00010010 00010011
        System.out.println("BytesDecoder" + frameBinary);

        // Init the BitSet with an array of reversed bytes to handle the little endian representation expected
        bitSetToDecode = BitSet.valueOf(reverse(bytes));
    }

    /**
     * Decode a part of the byte array between the startIndex and the endIndex
     *
     * @param startIndex index of the first bit to include
     * @param endIndex   index after the last bit to include
     * @return the int value of the decoded bits
     */
    public int decode(int startIndex, int endIndex) {
        int length = endIndex - startIndex;

        int decodedInt = convert(bitSetToDecode.get(startIndex, endIndex), length);

        System.out.println("BytesDecoder --- Decoded parameter --- ");
        System.out.println("BytesDecoder " + toBinary(decodedInt, length) + " interpreted as " + decodedInt);

        return decodedInt;
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

    /**
     * Get the binary form of an int
     *
     * @param num    the int number
     * @param length the bit length
     * @return the string value of the binary form of the int
     */
    private String toBinary(int num, int length) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < length; i++) {
            sb.append(((num & 1) == 1) ? '1' : '0');
            num >>= 1;
        }

        return sb.reverse().toString();
    }

    /**
     * Convert a BitSet into an int
     *
     * @param bits   the BitSet
     * @param length the BitSet theorical lenght
     * @return the int value corresponding
     */
    private int convert(BitSet bits, int length) {
        int value = 0;
        // Set the increment to the difference between the therocial length and the effective lenght of the bitset
        // to take into account the fact that the BitSet just represent significative bits
        // (i.e instead of 110, the bitset while contains 11 since the 0 is irrelevant in his representation)
        int increment = length - bits.length();

        // Browse the BitSet from the end to the begining to handle the little endian representation
        for (int i = bits.length() - 1; i >= 0; --i) {
            value += bits.get(i) ? (1L << increment) : 0L;
            increment++;
        }

        return value;
    }

}
