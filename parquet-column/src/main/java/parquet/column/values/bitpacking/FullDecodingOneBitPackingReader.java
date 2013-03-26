package parquet.column.values.bitpacking;

import java.io.IOException;
import java.io.InputStream;

import parquet.column.values.bitpacking.BitPacking.BitPackingReader;
import parquet.io.ParquetDecodingException;

public class FullDecodingOneBitPackingReader extends BitPackingReader {

  private final int[] decoded;
  private int index;

  public FullDecodingOneBitPackingReader(InputStream in, int[] decoded) {
    try {
      this.decoded = decoded;
      int valueCount = decoded.length;
      int bytesCount = (int)valueCount/8;
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