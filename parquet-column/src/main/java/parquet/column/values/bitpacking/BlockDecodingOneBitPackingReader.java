package parquet.column.values.bitpacking;

import java.io.IOException;
import java.io.InputStream;

import parquet.column.values.bitpacking.BitPacking.BitPackingReader;
import parquet.io.ParquetDecodingException;

public class BlockDecodingOneBitPackingReader extends BitPackingReader {

  private static final int BLOCK_SIZE = 64*1024;

  private final InputStream in;
  private int valueCount;

  private final byte[] decoded;
  private int index;

  public BlockDecodingOneBitPackingReader(InputStream in, int valueCount) {
    this.in = in;
    this.decoded = new byte[BLOCK_SIZE];
    this.valueCount = valueCount;
    this.index = BLOCK_SIZE;
  }

  private void readBlock() {
    try {
    if (valueCount > BLOCK_SIZE) {
      valueCount -= BLOCK_SIZE;
      for (int i = 0, j = 0; i < BLOCK_SIZE / 8; i++, j += 8) {
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
    } else {
      int toRead = valueCount;
      valueCount = 0;
      int bytesCount = (int)toRead / 8;
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
      int rest = (int)toRead % 8;
      if (rest > 0) {
        int b = in.read();
        for (int i = 0, j = (int)toRead - rest; j < toRead; i++, j++) {
          decoded[j] = (byte)((b >> 7 - i) & 1);
        }
      }
    }
    } catch (IOException e) {
      throw new ParquetDecodingException(e);
    }
    index = 0;
  }

  @Override
  public int read() throws IOException {
    if (index >= BLOCK_SIZE) {
      readBlock();
    }
    return decoded[index++];
  }

}