package parquet.column.values.bitpacking;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import parquet.column.values.bitpacking.BitPacking.BitPackingReader;
import parquet.column.values.bitpacking.BitPacking.BitPackingWriter;

public class BitPackingPerfTest {

  public enum ALG { STREAM, FULL_8, FULL_32 }

  public static void main(String[] args) throws IOException {
    int COUNT = 800000;
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
    for (int l = 0; l < 5; l++) {
    long s = slow(COUNT, bytes, result);
    long f = full(COUNT, bytes, result);
    System.out.println((float)f/s);
    long f32 = full32(COUNT, bytes, result);
    System.out.println((float)f32/s);
    }
  }

  private static long full(int COUNT, byte[] bytes, int[] result)
      throws IOException {
    return readNTimes(COUNT, bytes, result, ALG.FULL_8);
  }

  private static long full32(int COUNT, byte[] bytes, int[] result)
      throws IOException {
    return readNTimes(COUNT, bytes, result, ALG.FULL_32);
  }

  private static long readNTimes(int COUNT, byte[] bytes, int[] result, ALG alg)
      throws IOException {
    System.out.println();
    System.out.println(alg);
    long t = 0;
    for (int l = 0; l < 1; l++) {
      System.gc();
      System.out.print("<");
      long t2 = System.currentTimeMillis();
      for (int k = 0; k < 1000; k++) {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        BitPackingReader r;
        switch (alg) {
          case STREAM:
            r = BitPacking.createBitPackingReader(1, bais, COUNT);
            break;
          case FULL_8:
            r = new FullDecodingOneBitPackingReader(bais, result);
            break;
          case FULL_32:
            r = new Full32DecodingOneBitPackingReader(bais, result);
            break;
            default: throw new RuntimeException(alg + " unknown");
        }
        for (int i = 0; i < result.length; i++) {
          result[i] = r.read();
        }
      }
      long t3 = System.currentTimeMillis();
      t = t3 - t2;
      System.out.println("> read in " + t + "ms");
    }
    return t;
  }

  private static long slow(int COUNT, byte[] bytes, int[] result)
      throws IOException {
    return readNTimes(COUNT, bytes, result, ALG.STREAM);
  }
}
