package org.amity.helper;

import java.util.Iterator;

/**
 *
 */
public class ByteArrayBitIterable implements Iterable<Boolean> {
    public static final int BYTE_SIZE = 8;
    private final byte[] array;

    public ByteArrayBitIterable(byte[] array) {
        this.array = array;
    }

    public Iterator<Boolean> iterator() {
        return new Iterator<>() {
            private int bitIndex = 0;
            private int arrayIndex = 0;

            public boolean hasNext() {
                return (arrayIndex < array.length) && (bitIndex < BYTE_SIZE);
            }

            public Boolean next() {

                Boolean val = (array[arrayIndex] >> (7 - bitIndex) & 1) == 1;
                bitIndex++;
                if (bitIndex == BYTE_SIZE) {
                    bitIndex = 0;
                    arrayIndex++;
                }
                return val;
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static void main(String[] a) {
        ByteArrayBitIterable test = new ByteArrayBitIterable(
//                new byte[]{(byte)0xAA, (byte)0xAA});
//                0x12 -> 0b00010010 -> 18
//                0x13 -> 0b00010011 -> 19
                new byte[]{(byte) 0x12, (byte) 0x13});
        for (boolean b : test)
            System.out.println(b);
    }
}