package de.jhoff.dmap;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.jhoff.dmap.util.ByteArray;

/**
 * Builder for the DMap. The DMapBuilder is a write-once builder, as DMap is
 * read-only.
 * 
 * Improve:
 *  - Spill keys to disk every K bytes.
 *  - Make appendable.
 *  - Make iterable.
 *  - Compress using varint or delta-encoding
 */
public class DMapBuilder {
 
  /** Keeps the byte offsets in the file of each value added. */
  private Map<ByteArray, Integer> valueOffsets_;

  /** Map file to write to. */
  private File mapFile_;
  
  /** Writer to the map file. */
  private DataOutputStream output_;

  /** Current offset in the file. */
  private int currentOffset_;
    
  private Logger logger_ = LoggerFactory.getLogger(DMapBuilder.class);

  public DMapBuilder(File mapFile) throws IOException {
    boolean success = mapFile.createNewFile();
    if (success) {
      mapFile_ = mapFile;
      output_ = new DataOutputStream(
                  new BufferedOutputStream(
                    new FileOutputStream(mapFile), 1024));
      valueOffsets_ = new HashMap<>();
      
      // Write placeholder for number of entries and start of the offsets.
      output_.writeInt(0);
      output_.writeInt(0);
      currentOffset_ = 8;
    } else {
      throw new IOException("Output map file already exists at: " + mapFile
          + ", cannot write.");
    }
  }

  public void add(byte[] key, byte[] value) throws IOException {
    ByteArray keyBytes = new ByteArray(Arrays.copyOf(key, key.length));
    if (valueOffsets_.containsKey(keyBytes)) {
      logger_.warn(
          "Key '" + key + "' has already been added, not adding again");
    } else {
      logger_.debug("write@" + currentOffset_ + ": " + value.length +
          "\t" + value);
      output_.writeInt(value.length);           
      output_.write(value);
      // Keep track of current offset for the value.
      valueOffsets_.put(keyBytes, currentOffset_);
      // Increment the offset to point to after the current value.
      currentOffset_ += (4 + value.length);
    }
  }

  public void build() throws IOException {
    List<ByteArray> allKeys = new ArrayList<>(valueOffsets_.keySet());
    Collections.sort(allKeys);

    logger_.info("Writing map for " + allKeys.size() + " keys.");
    
    for (ByteArray keyBytes : allKeys) {
      // Write size + key.
      byte[] key = keyBytes.getBytes();
      logger_.debug("write keylength: " + key.length);
      output_.writeInt(key.length);
      logger_.debug("write key: " + keyBytes + ""
          + " (hash: " + keyBytes.hashCode() + ")");
      output_.write(key);
      
      // Write offset.
      int offset = valueOffsets_.get(keyBytes);
      logger_.debug("write offset: " + offset);
      output_.writeInt(offset);
    }
    output_.flush();
    output_.close();
    
    // Write entry count and file offset of keys.
    RandomAccessFile raf = new RandomAccessFile(mapFile_, "rw");   
    raf.writeInt(valueOffsets_.size());    
    raf.seek(4);
    logger_.info("Key offsets start at " + currentOffset_ + ".");
    raf.writeInt(currentOffset_);
    raf.close();
  }
}
