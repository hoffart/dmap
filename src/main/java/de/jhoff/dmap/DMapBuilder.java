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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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

  /** Current Map File Version */
  private static final int VERSION = 1;

  /** Default Key-Value Block size - set to 1 MB. */
  private static final int DEFAULT_BLOCK_SIZE = 1048576;

  /** Current block size for the file*/
  private int blockSize;

  /** Keeps the keys read to avoid reading duplicates. */
  private Set<ByteArray> keyBytes_;

  /** Map file to write to. */
  private File mapFile_;

  /** Writer to the map file. */
  private DataOutputStream output_;

  /** Temporary Map file. */
  private File tmpMapFile_;

  /** Writer to the temp file. */
  private DataOutputStream tmpOutput_;

  private Logger logger_ = LoggerFactory.getLogger(DMapBuilder.class);

  public DMapBuilder(File mapFile) throws IOException {
    this(mapFile, DEFAULT_BLOCK_SIZE);
  }

  public DMapBuilder(File mapFile, int block) throws IOException {
    boolean success = mapFile.createNewFile();
    if (success) {
      blockSize = block;
      mapFile_ = mapFile;
      tmpMapFile_ = new File("tmp_" + mapFile.getName());
      tmpOutput_ = new DataOutputStream(
                  new BufferedOutputStream(
                    new FileOutputStream(tmpMapFile_), 1024));
      output_ = new DataOutputStream(
          new BufferedOutputStream(
              new FileOutputStream(mapFile_), 1024));
      
      keyBytes_ = new HashSet<>();
    } else {
      throw new IOException("Output map file already exists at: " + mapFile
          + ", cannot write.");
    }    
  }

  public void add(byte[] key, byte[] value) throws IOException {
    ByteArray keyBytes = new ByteArray(Arrays.copyOf(key, key.length));
    if (keyBytes_.contains(keyBytes)) {
      logger_.warn(
          "Key '" + key + "' has already been added, not adding again");
    } else {
      //logger_.debug("write@ : " + value.length + "\t" + value);
      tmpOutput_.writeInt(key.length);
      tmpOutput_.writeInt(value.length);
      tmpOutput_.write(key);
      tmpOutput_.write(value);
      // Keep track of key
      keyBytes_.add(keyBytes);      
    }
  }

  public void build() throws IOException {
    Map<ByteArray, ByteArray> tmpKVMap =  new HashMap<ByteArray, ByteArray>();
    tmpOutput_.flush();
    tmpOutput_.close();

    RandomAccessFile raf = new RandomAccessFile(tmpMapFile_, "r");
    byte[] key;
    byte[] value;
    for(int i=0;i<keyBytes_.size();i++) {
      int keyLen = raf.readInt();
      int valLen = raf.readInt();
      key = new byte[keyLen];
      value = new byte[valLen];
      raf.read(key);
      raf.read(value);
      tmpKVMap.put(new ByteArray(key), new ByteArray(value));
    }
    raf.close();

    logger_.debug("Loaded " + tmpKVMap.size() + " entries from temporary file");

    // global header - version, entries count, block size, trailer offset
    output_.writeInt(VERSION);
    output_.writeInt(tmpKVMap.size());
    output_.writeInt(blockSize);
    // insert placeholder for trailer offset
    output_.writeInt(0);
    List<ByteArray> allKeys = new ArrayList<>(tmpKVMap.keySet());
    Collections.sort(allKeys);    
    logger_.info("Writing map for " + allKeys.size() + " keys.");

    int globalOffset = 16;        
    int currentBlockOffset = 0;
    int remainingBytes = blockSize;
    Map<ByteArray, Integer> keyOffset_ = new HashMap<>();
    Map<Integer, Integer> blockTrailerOffsets = new HashMap<>();

    for (ByteArray keyBytes : allKeys) {
      key = keyBytes.getBytes();
      ByteArray valueBytes = tmpKVMap.get(keyBytes);
      value = valueBytes.getBytes();

      int dataLength = 4 + value.length;;
      // write block trailer & reset variables
      if(dataLength > remainingBytes) {
        logger_.debug("Key : " + keyBytes + " with value doesnt fit in remaining "+ remainingBytes + " bytes.");        
        globalOffset = updateBlockTrailer(keyOffset_, blockTrailerOffsets, globalOffset);        
        logger_.debug("Creating new block @ " + globalOffset);
        currentBlockOffset = 0;
        remainingBytes = blockSize;        
      }

      logger_.debug("write@ "+ globalOffset +" key: " + keyBytes + ""
          + " (hash: " + keyBytes.hashCode() + ") with value: " + valueBytes);      
      output_.writeInt(value.length);
      // write value (key can be retrieved from block trailer)
      output_.write(value);
      // store key-offset pair (needed for block trailer)
      keyOffset_.put(keyBytes, currentBlockOffset);
      currentBlockOffset += (dataLength);
      remainingBytes -= dataLength;
    }

    // write the last block trailer information
    globalOffset = updateBlockTrailer(keyOffset_, blockTrailerOffsets, globalOffset); 

    // write global trailer (block start offset-block trailer offset pair)
    output_.writeInt(blockTrailerOffsets.size());
    for(Entry<Integer, Integer> e : blockTrailerOffsets.entrySet()) {
      output_.writeInt(e.getKey());
      output_.writeInt(e.getValue());
    }
    output_.flush();
    output_.close();
    
    // fill in the previously created placeholder for trailer offset
    raf = new RandomAccessFile(mapFile_, "rw");   
    raf.seek(12);
    logger_.info("DMap Trailer start at " + globalOffset + ".");
    raf.writeInt(globalOffset);
    raf.close();
        
    // delete the intermediate temp file
    tmpMapFile_.delete();
    logger_.info("Done.");
  }
  
  /*
   * Returns new global offset after updating the block trailer 
   */
  private int updateBlockTrailer(Map<ByteArray, Integer> keyOffsets, Map<Integer, Integer> blockTrailerOffsets, int globalOffset) throws IOException {
    int trailerOffset = output_.size();
    // write number of entries in the current block
    output_.writeInt(keyOffsets.size());
    for(Entry<ByteArray, Integer> e : keyOffsets.entrySet()) {
      ByteArray byteArray = e.getKey();
      output_.writeInt(byteArray.getBytes().length);
      output_.write(byteArray.getBytes());
      output_.writeInt(keyOffsets.get(byteArray));
    }
    keyOffsets.clear();
    // track block offset info
    blockTrailerOffsets.put(globalOffset, trailerOffset);
    return output_.size();        
  }
}