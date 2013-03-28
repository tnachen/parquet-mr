package parquet.column.values.bitpacking;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import parquet.column.values.bitpacking.BitPacking.BitPackingReader;
import parquet.column.values.bitpacking.BitPacking.BitPackingWriter;

public class BitPackingPerfTest {

  public enum ALG { STREAM, BLOCK, FULL_8, FULL_32 }

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
      long b = block(COUNT, bytes, result);
      System.out.println((float)b/s);
      //    long f = full(COUNT, bytes, result);
      //    System.out.println((float)f/s);
      //    long f32 = full32(COUNT, bytes, result);
      //    System.out.println((float)f32/s);
    }
  }

  private static long full(int count, byte[] bytes, int[] result)
      throws IOException {
    return readNTimes(count, bytes, result, ALG.FULL_8);
  }

  private static long block(int count, byte[] bytes, int[] result)
      throws IOException {
    return readNTimes(count, bytes, result, ALG.BLOCK);
  }

  private static long full32(int count, byte[] bytes, int[] result)
      throws IOException {
    return readNTimes(count, bytes, result, ALG.FULL_32);
  }

  private static long readNTimes(int count, byte[] bytes, int[] result, ALG alg)
      throws IOException {
    System.out.println();
    System.out.println(alg);
    long t = 0;
    int N = 10;
    ByteArrayInputStream bais;
    System.gc();
    System.out.print("no gc <");
    for (int k = 0; k < N; k++) {
      BitPackingReader r;
      long t2 = System.nanoTime();
      switch (alg) {
      case STREAM:
        r = BitPacking.createBitPackingReader(1, bytes, 0, bytes.length, count);
        break;
      case BLOCK:
        r = new BlockDecodingOneBitPackingReader(bytes, 0, count);
        break;
      case FULL_8:
        bais = new ByteArrayInputStream(bytes);
        r = new FullDecodingOneBitPackingReader(bais, result);
        break;
      case FULL_32:
        bais = new ByteArrayInputStream(bytes);
        r = new Full32DecodingOneBitPackingReader(bais, result);
        break;
      default: throw new RuntimeException(alg + " unknown");
      }
      for (int i = 0; i < result.length; i++) {
        result[i] = r.read();
      }
      long t3 = System.nanoTime();
      t += t3 - t2;
    }
    System.out.println("> read in " + t/1000 + "µs " + (N * result.length / (t / 1000)) + " values per µs");
    return t;
  }

  private static long slow(int count, byte[] bytes, int[] result)
      throws IOException {
    return readNTimes(count, bytes, result, ALG.STREAM);
  }
}
