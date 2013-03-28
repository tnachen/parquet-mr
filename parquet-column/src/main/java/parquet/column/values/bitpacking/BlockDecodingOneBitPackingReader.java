package parquet.column.values.bitpacking;

import java.io.IOException;

import parquet.Log;
import parquet.column.values.bitpacking.BitPacking.BitPackingReader;

public class BlockDecodingOneBitPackingReader extends BitPackingReader {
  private static final Log LOG = Log.getLog(BlockDecodingOneBitPackingReader.class);

  private static final int BLOCK_SIZE = 8*1024; // must be a multiple of 32

  private final byte[] bytes;
  private int offset;
  private long valueCount;

  private final byte[] decoded;
  private int index;


  public BlockDecodingOneBitPackingReader(byte[] bytes, int offset, long valueCount) {
    this.bytes = bytes;
    this.offset = offset;
    this.decoded = new byte[BLOCK_SIZE];
    this.valueCount = valueCount;
    this.index = BLOCK_SIZE;
  }

  private void readBlock() {
    if (valueCount > BLOCK_SIZE) {
      if (Log.DEBUG) LOG.debug("read block " + valueCount);
      for (int i = offset, j = 0; j < BLOCK_SIZE; j += 32, i += 4) {
        decoded[j +  0] = (byte)(((int) bytes[i + 0] >> 7) & 1);
        decoded[j +  1] = (byte)(((int) bytes[i + 0] >> 6) & 1);
        decoded[j +  2] = (byte)(((int) bytes[i + 0] >> 5) & 1);
        decoded[j +  3] = (byte)(((int) bytes[i + 0] >> 4) & 1);
        decoded[j +  4] = (byte)(((int) bytes[i + 0] >> 3) & 1);
        decoded[j +  5] = (byte)(((int) bytes[i + 0] >> 2) & 1);
        decoded[j +  6] = (byte)(((int) bytes[i + 0] >> 1) & 1);
        decoded[j +  7] = (byte)(((int) bytes[i + 0] >> 0) & 1);

        decoded[j +  8] = (byte)(((int) bytes[i + 1] >> 7) & 1);
        decoded[j +  9] = (byte)(((int) bytes[i + 1] >> 6) & 1);
        decoded[j + 10] = (byte)(((int) bytes[i + 1] >> 5) & 1);
        decoded[j + 11] = (byte)(((int) bytes[i + 1] >> 4) & 1);
        decoded[j + 12] = (byte)(((int) bytes[i + 1] >> 3) & 1);
        decoded[j + 13] = (byte)(((int) bytes[i + 1] >> 2) & 1);
        decoded[j + 14] = (byte)(((int) bytes[i + 1] >> 1) & 1);
        decoded[j + 15] = (byte)(((int) bytes[i + 1] >> 0) & 1);

        decoded[j + 16] = (byte)(((int) bytes[i + 2] >> 7) & 1);
        decoded[j + 17] = (byte)(((int) bytes[i + 2] >> 6) & 1);
        decoded[j + 18] = (byte)(((int) bytes[i + 2] >> 5) & 1);
        decoded[j + 19] = (byte)(((int) bytes[i + 2] >> 4) & 1);
        decoded[j + 20] = (byte)(((int) bytes[i + 2] >> 3) & 1);
        decoded[j + 21] = (byte)(((int) bytes[i + 2] >> 2) & 1);
        decoded[j + 22] = (byte)(((int) bytes[i + 2] >> 1) & 1);
        decoded[j + 23] = (byte)(((int) bytes[i + 2] >> 0) & 1);

        decoded[j + 24] = (byte)(((int) bytes[i + 3] >> 7) & 1);
        decoded[j + 25] = (byte)(((int) bytes[i + 3] >> 6) & 1);
        decoded[j + 26] = (byte)(((int) bytes[i + 3] >> 5) & 1);
        decoded[j + 27] = (byte)(((int) bytes[i + 3] >> 4) & 1);
        decoded[j + 28] = (byte)(((int) bytes[i + 3] >> 3) & 1);
        decoded[j + 29] = (byte)(((int) bytes[i + 3] >> 2) & 1);
        decoded[j + 30] = (byte)(((int) bytes[i + 3] >> 1) & 1);
        decoded[j + 31] = (byte)(((int) bytes[i + 3] >> 0) & 1);
      }
      valueCount -= BLOCK_SIZE;
      offset += BLOCK_SIZE / 8;
    } else {
      if (Log.DEBUG) LOG.debug("read less bytes than a block " + valueCount);
      int toRead = (int)valueCount;
      int bytesCount = (int)toRead / 8;
      for (int i = offset, j = 0; i < offset + bytesCount; ++i, j += 8) {
        decoded[j + 0] = (byte)((bytes[i] >> 7) & 1);
        decoded[j + 1] = (byte)((bytes[i] >> 6) & 1);
        decoded[j + 2] = (byte)((bytes[i] >> 5) & 1);
        decoded[j + 3] = (byte)((bytes[i] >> 4) & 1);
        decoded[j + 4] = (byte)((bytes[i] >> 3) & 1);
        decoded[j + 5] = (byte)((bytes[i] >> 2) & 1);
        decoded[j + 6] = (byte)((bytes[i] >> 1) & 1);
        decoded[j + 7] = (byte)((bytes[i] >> 0) & 1);
      }
      offset += bytesCount;
      int rest = (int)toRead % 8;
      if (rest > 0) {
        if (Log.DEBUG) LOG.debug("read last byte " + rest);
        for (int i = 0, j = (int)toRead - rest; j < toRead; i++, j++) {
          decoded[j] = (byte)((bytes[offset] >> 7 - i) & 1);
        }
      }
      valueCount = 0;
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