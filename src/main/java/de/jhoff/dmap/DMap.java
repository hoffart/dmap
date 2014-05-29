package de.jhoff.dmap;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.jhoff.dmap.util.ByteArray;

/**
 * Disk-backed implementation of a very simple Map that supports only 
 */
public class DMap {
  /** Map file with data. */
  private File mapFile_;
  private RandomAccessFile raf_;
  
  /** Memory map of mapFile_. */
  private MappedByteBuffer data_;
    
  /** Number of entries in the map. */
  private int size;
  
  private Map<ByteArray, Integer> valueOffsets_;
  
  private Logger logger_ = LoggerFactory.getLogger(DMap.class);
  
  /**
   * Default constructor, will preload key/offsets but not the values.
   * 
   * @param mapFile Map file created by DMapBuilder
   * @throws IOException 
   */
  public DMap(File mapFile) throws IOException {
    this(mapFile, true, false);
  }
  
  public DMap(File mapFile, boolean preloadOffsets, boolean preloadValues) 
      throws IOException {
    mapFile_ = mapFile;
    raf_ = new RandomAccessFile(mapFile_, "r");
    FileChannel fc = raf_.getChannel();
    
    data_ = fc.map(FileChannel.MapMode.READ_ONLY, 0, mapFile.length());
    
    size = data_.getInt(0);
    
    if (preloadOffsets) {
      valueOffsets_ = loadOffsets(data_);
    }
    
    if (preloadValues) {
      data_.load();
    }
  }
  
  private Map<ByteArray, Integer> loadOffsets(MappedByteBuffer buf) {
    int entryCount = buf.getInt(0);
    int keyOffset = buf.getInt(4);   
    
    Map<ByteArray, Integer> offsets = new HashMap<>(entryCount);
    for (int i = 0; i < entryCount; ++i) {
      // Read key length.
      int keyLength = buf.getInt(keyOffset);
      
      // Read key.
      keyOffset += 4;
      byte[] key = new byte[keyLength];
      buf.position(keyOffset);
      buf.get(key);
      
      // Read byte offset of value.
      keyOffset += keyLength;
      Integer valueOffset = buf.getInt(keyOffset);
      
      // Set keyOffset to next entry.
      keyOffset += 4;
      ByteArray keyBytes = new ByteArray(key); 
      logger_.debug("read-key-offset " + keyBytes + "\t" + valueOffset);
      offsets.put(keyBytes, valueOffset);
    }
    
    return offsets;
  }
  
  /**
   * Get the number of entries in the map.
   * 
   * @return Number of entries in the map.
   */
  public int size() {
    return size;
  }

  /**
   * Get byte[] value for key. TODO this could be made more efficient
   * by returning a reference to the original byte array in memory!
   * 
   * @param key Key to retrieve the value for.
   * @return  byte[] associated with key.
   */
  public byte[] get(byte[] key) {
    if (valueOffsets_ == null) {
      // TODO implement on-disk search.
      throw new IllegalStateException(
          "preloadOffsets has to be true in the constructor for now.");
    }
    ByteArray keyBytes = new ByteArray(key);
    logger_.debug("get(" + keyBytes + ") - hash: " + keyBytes.hashCode());
    Integer valueOffset = valueOffsets_.get(keyBytes);
    if (valueOffset == null) {
      return null;
    } else {
      int valueLength = data_.getInt(valueOffset);
      byte[] value = new byte[valueLength];
      // Threadsafe, stateless access to the ByteBuffer.
      valueOffset += 4;
      for (int i = 0; i < value.length; ++i) {
        value[i] = data_.get(valueOffset);
        ++valueOffset;
      }
      return value;
    }
  }
}