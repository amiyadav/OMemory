package org.amity.test;
// Java program to extract k bits from a given
// position.
 
class GFG {
 
    // Function to extract k bits from p position
    // and returns the extracted value as integer
    static int bitExtracted(int number, int k, int p)
    {
        return (((1 << k) - 1) & (number >> (p - 1)));
    }
  
    // Driver code
    public static void main (String[] args) {
//        int number = 171, k = 5, p = 2;
        int number = 72, k = 5, p = 1;
        System.out.println("The extracted number is "+
               bitExtracted(number, k, p));

        byte[] b = new byte[]{0x12, 0x13};
        for (int i = 0 ; i < b.length ; i++ ) {
            Byte bt = b[0];
//            bt.
        }
    }
}