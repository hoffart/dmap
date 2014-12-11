package de.jhoff.dmap.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by fkeller on 11/26/14.
 */
public class ExtendedFileChannel {
  private FileChannel fileChannel_;
  private final ByteBuffer intBuffer_;
  private final ByteBuffer longBuffer_;

  public ExtendedFileChannel(FileChannel fileChannel) {
    fileChannel_ = fileChannel;
    intBuffer_ = ByteBuffer.allocate(4);
    longBuffer_ = ByteBuffer.allocate(8);
  }
  
  public int write(ByteBuffer byteBuffer) throws IOException {
    byteBuffer.position(0);
    return fileChannel_.write(byteBuffer);
  }
  
  public int write(byte[] bytes) throws IOException {
    return write(ByteBuffer.wrap(bytes));
  }
  
  public int writeInt(int value) throws IOException {
    intBuffer_.rewind();
    intBuffer_.putInt(value);
    return write(intBuffer_);
  }

  public int writeLong(long value) throws IOException {
    longBuffer_.rewind();
    longBuffer_.putLong(value);
    return write(longBuffer_);
  }
  
  public int read(ByteBuffer byteBuffer) throws IOException {
    return fileChannel_.read(byteBuffer);
  }
  
  public int read(byte[] bytes) throws IOException {
    return read(ByteBuffer.wrap(bytes));
  }
  
  public int readInt() throws IOException {
    intBuffer_.rewind();
    read(intBuffer_);
    return intBuffer_.getInt(0);
  }
  
  public long readLong() throws IOException {
    longBuffer_.rewind();
    read(longBuffer_);
    return longBuffer_.getLong(0);
  }
  
  public void flush() throws IOException {
    fileChannel_.force(true);
  }
  
  public void close() throws IOException {
    fileChannel_.close();
  }
  
  public long size() throws IOException {
    return fileChannel_.size();
  }
  
  public long position() throws IOException {
    return fileChannel_.position();
  }
  
  public ExtendedFileChannel position(long newPosition) throws IOException {
    fileChannel_.position(newPosition);
    return this;
  }
  
  public MappedByteBuffer map(FileChannel.MapMode mapMode, long position, long size) throws IOException {
    return fileChannel_.map(mapMode, position, size);
  }
}
