package parquet.hadoop.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.CompressorStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DecompressorStream;

/**
 * Snappy compression codec for parquet.  We do not use the default hadoop
 * one since that codec adds a blocking structure around the base snappy compression
 * algorithm.  This is useful for hadoop to minimize the size of comrpession blocks
 * for their file formats (e.g. SequenceFile) but is undesirable for parquet since
 * we already have the data page which provides that.
 */
public class SnappyCodec implements Configurable, CompressionCodec {
  Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public Compressor createCompressor() {
    return new SnappyCompressor();
  }

  @Override
  public Decompressor createDecompressor() {
    return new SnappyDecompressor();
  }

  @Override
  public CompressionInputStream createInputStream(InputStream stream)
      throws IOException {
    return createInputStream(stream, createDecompressor());
  }

  @Override
  public CompressionInputStream createInputStream(InputStream stream,
      Decompressor decompressor) throws IOException {
    return new DecompressorStream(stream, decompressor,
        conf.getInt("io.file.buffer.size", 4*1024));
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream stream)
      throws IOException {
    return createOutputStream(stream, createCompressor());
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream stream,
      Compressor compressor) throws IOException {
    return new CompressorStream(stream, compressor, 
        conf.getInt("io.file.buffer.size", 4*1024));
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    return SnappyCompressor.class;
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return SnappyDecompressor.class;
  }

  @Override
  public String getDefaultExtension() {
    return ".snappy";
  }
}
