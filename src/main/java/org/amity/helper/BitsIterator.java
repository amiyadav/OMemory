package org.amity.helper;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

public class BitsIterator implements Iterable<Boolean> {
    public static final int BYTE_SIZE = 8;
    private final byte[] array;

    public BitsIterator(byte[] array) {
        this.array = array;
    }

    @Override
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
    @Override
    public void forEach(Consumer<? super Boolean> action) {
        Iterable.super.forEach(action);
    }

    @Override
    public Spliterator<Boolean> spliterator() {
        return Iterable.super.spliterator();
    }

}
