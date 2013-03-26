package parquet.column.values.bitpacking;

import java.io.IOException;
import java.io.InputStream;

import parquet.column.values.bitpacking.BitPacking.BitPackingReader;
import parquet.io.ParquetDecodingException;

public class Full32DecodingOneBitPackingReader extends BitPackingReader {

  private final int[] decoded;
  private int index;

  public Full32DecodingOneBitPackingReader(InputStream in, int[] decoded) {
    try {
      this.decoded = decoded;
      int valueCount = decoded.length;
      byte[] buffer = new byte[4];
      int bytes4Count = (int)valueCount/32;
      for (int i = 0, j = 0; i < bytes4Count; i++, j += 32) {
        in.read(buffer);
        byte b = buffer[0];
        decoded[j +  0] = (byte)((b >> 7) & 1);
        decoded[j +  1] = (byte)((b >> 6) & 1);
        decoded[j +  2] = (byte)((b >> 5) & 1);
        decoded[j +  3] = (byte)((b >> 4) & 1);
        decoded[j +  4] = (byte)((b >> 3) & 1);
        decoded[j +  5] = (byte)((b >> 2) & 1);
        decoded[j +  6] = (byte)((b >> 1) & 1);
        decoded[j +  7] = (byte)((b >> 0) & 1);
        b = buffer[1];
        decoded[j +  8] = (byte)((b >> 7) & 1);
        decoded[j +  9] = (byte)((b >> 6) & 1);
        decoded[j + 10] = (byte)((b >> 5) & 1);
        decoded[j + 11] = (byte)((b >> 4) & 1);
        decoded[j + 12] = (byte)((b >> 3) & 1);
        decoded[j + 13] = (byte)((b >> 2) & 1);
        decoded[j + 14] = (byte)((b >> 1) & 1);
        decoded[j + 15] = (byte)((b >> 0) & 1);
        b = buffer[2];
        decoded[j + 16] = (byte)((b >> 7) & 1);
        decoded[j + 17] = (byte)((b >> 6) & 1);
        decoded[j + 18] = (byte)((b >> 5) & 1);
        decoded[j + 19] = (byte)((b >> 4) & 1);
        decoded[j + 20] = (byte)((b >> 3) & 1);
        decoded[j + 21] = (byte)((b >> 2) & 1);
        decoded[j + 22] = (byte)((b >> 1) & 1);
        decoded[j + 23] = (byte)((b >> 0) & 1);
        b = buffer[3];
        decoded[j + 24] = (byte)((b >> 7) & 1);
        decoded[j + 25] = (byte)((b >> 6) & 1);
        decoded[j + 26] = (byte)((b >> 5) & 1);
        decoded[j + 27] = (byte)((b >> 4) & 1);
        decoded[j + 28] = (byte)((b >> 3) & 1);
        decoded[j + 29] = (byte)((b >> 2) & 1);
        decoded[j + 30] = (byte)((b >> 1) & 1);
        decoded[j + 31] = (byte)((b >> 0) & 1);
      }
      valueCount = valueCount - bytes4Count * 32;
      int bytesCount = (int)(valueCount)/8;
      for (int i = 0, j = 0; i < bytesCount; i++, j += 8) {
        int b = in.read();
        decoded[j + 0] = (byte)((b >> 7) & 1);
        decoded[j + 1] = (byte)((b >> 6) & 1);
        decoded[j + 2] = (byte)((b >> 5) & 1);
        decoded[j + 3] = (byte)((b >> 4) & 1);
        decoded[j + 4] = (byte)((b >> 3) & 1);
        decoded[j + 5] = (byte)((b >> 2) & 1);
        decoded[j + 6] = (byte)((b >> 1) & 1);
        decoded[j + 7] = (byte)((b >> 0) & 1);
      }

      int rest = (int)valueCount % 8;
      if (rest > 0) {
        int b = in.read();
        for (int i = 0, j = (int)valueCount - rest; j < valueCount; i++, j++) {
          decoded[j] = (byte)((b >> 7 - i) & 1);
        }
      }
    } catch (IOException e) {
      throw new ParquetDecodingException(e);
    }
  }

  @Override
  public int read() throws IOException {
    return decoded[index++];
  }

}