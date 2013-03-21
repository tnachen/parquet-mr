package parquet.column.values.bitpacking;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import parquet.column.values.bitpacking.BitPacking.BitPackingReader;
import parquet.column.values.bitpacking.BitPacking.BitPackingWriter;

public class BitPackingPerfTest {

  public static void main(String[] args) throws IOException {
    int COUNT = 8000;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BitPackingWriter w = BitPacking.getBitPackingWriter(1, baos);
    long t0 = System.currentTimeMillis();
    for (int i = 0 ; i < COUNT; ++i) {
      w.write(i % 2);
    }
    w.finish();
    long t1 = System.currentTimeMillis();
    System.out.println("written in " + (t1 - t0) + "ms");
    System.out.println();
    byte[] bytes = baos.toByteArray();
    int[] result = new int[COUNT];
    slow(COUNT, bytes, result);
    System.out.println();
    fast(COUNT, bytes, result);
  }

  private static void fast(int COUNT, byte[] bytes, int[] result)
      throws IOException {
    BitPacking.FULL = true;
    System.out.println("fast");
    readNTimes(COUNT, bytes, result);
  }

  private static void readNTimes(int COUNT, byte[] bytes, int[] result)
      throws IOException {
    for (int l = 0; l < 3; l++) {
      long t2 = System.currentTimeMillis();
      for (int k = 0; k < 10; k++) {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        BitPackingReader r = BitPacking.createBitPackingReader(1, bais, COUNT);
        for (int i = 0; i < result.length; i++) {
          result[i] = r.read();
        }
      }
      long t3 = System.currentTimeMillis();
      System.out.println("read in " + (t3 - t2) + "ms");
    }
  }

  private static void slow(int COUNT, byte[] bytes, int[] result)
      throws IOException {
    BitPacking.FULL = false;
    System.out.println("slow");
    readNTimes(COUNT, bytes, result);
  }
}
