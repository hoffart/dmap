package de.jhoff.dmap;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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

  /** Default Key-Value Block size (in bytes) - set to 1 MB. */
  private static final int DEFAULT_BLOCK_SIZE = 1048576;

  /** Current block size for the file*/
  private int blockSize;

  /** Map file to write to. */
  private File mapFile_;

  /** Writer to the map file. */
  private DataOutputStream output_;

  /** Temporary Map file. */
  private File tmpMapFile_;

  /** Writer to the temp file. */
  private DataOutputStream tmpOutput_;

  /** Keep track of number of entries retrieved from original file */
  private int entriesCount_;
  
  private Logger logger_ = LoggerFactory.getLogger(DMapBuilder.class);

  public DMapBuilder(File mapFile) throws IOException {
    this(mapFile, DEFAULT_BLOCK_SIZE);
  }

  /**
   * 
   * @param mapFile Map File instance.
   * @param 
   * @throws IOException
   */
  public DMapBuilder(File mapFile, int block) throws IOException {
    boolean success = mapFile.createNewFile();
    if (success) {
      blockSize = block;
      mapFile_ = mapFile;
      tmpMapFile_ = new File("tmp_" + mapFile.getName());
      success = tmpMapFile_.createNewFile();
      if(success) {
        tmpOutput_ = new DataOutputStream(
            new BufferedOutputStream(
              new FileOutputStream(tmpMapFile_), 1024));
      } else {
        throw new IOException("Error creating intermediate file: " + tmpMapFile_ + ", cannot write.");
      }
      
      output_ = new DataOutputStream(
          new BufferedOutputStream(
              new FileOutputStream(mapFile_), 1024));            
    } else {
      throw new IOException("Output map file already exists at: " + mapFile
          + ", cannot write.");
    }    
  }

  public void add(byte[] key, byte[] value) throws IOException {
    tmpOutput_.writeInt(key.length);
    tmpOutput_.writeInt(value.length);
    tmpOutput_.write(key);
    tmpOutput_.write(value);
    entriesCount_++;
  }

  public void build() throws IOException {
    Map<ByteArray, Long> tmpKeyOffsetMap =  new HashMap<ByteArray, Long>();
    tmpOutput_.flush();
    tmpOutput_.close();

    RandomAccessFile raf = new RandomAccessFile(tmpMapFile_, "r");
    byte[] key;
    byte[] value;
    long currentOffset = 0;
    
    try {
      logger_.debug("Keys to process: " + entriesCount_);
      for(int i=0;i<entriesCount_;i++) {
        int keyLen = raf.readInt();
        int valLen = raf.readInt();
        
        key = new byte[keyLen];      
        raf.read(key);      
        if(tmpKeyOffsetMap.containsKey(new ByteArray(key))) {
          throw new IOException("Duplicate key encountered: " + key);
        }
        
        tmpKeyOffsetMap.put(new ByteArray(key), currentOffset);
        // ignore the value byte sequence (for the time being) and move to next record start pos
        currentOffset = raf.getFilePointer() + valLen;
        raf.seek(currentOffset);
      }

      logger_.debug("Loaded " + tmpKeyOffsetMap.size() + " keys from temporary file");

      // global header - version, entries count, block size, trailer offset
      output_.writeInt(VERSION);
      output_.writeInt(tmpKeyOffsetMap.size());
      output_.writeInt(blockSize);
      // insert placeholder for trailer offset
      output_.writeInt(0);
      
      List<ByteArray> allKeys = new ArrayList<>(tmpKeyOffsetMap.keySet());
      Collections.sort(allKeys);    
      logger_.info("Writing map for " + allKeys.size() + " keys.");

      int globalOffset = 16;        
      int currentBlockOffset = 0;
      int remainingBytes = blockSize;
      // Map to store block-level key-offset pairs (to be written to each block trailer) 
      Map<ByteArray, Integer> blockKeyOffset_ = new HashMap<>();
      // Map to store blockStart-blockTrailerStart pair (to be written to global trailer)
      Map<Integer, Integer> blockTrailerOffsets = new HashMap<>();

      for (ByteArray keyBytes : allKeys) {
        key = keyBytes.getBytes();
        long offset = tmpKeyOffsetMap.get(keyBytes);
        raf.seek(offset);
        int keyLen = raf.readInt();
        int valLen = raf.readInt();
        value = new byte[valLen];
        // position pointer at the starting of value data
        raf.seek(raf.getFilePointer() + keyLen);
        raf.read(value);

        int dataLength = 4 + value.length;

        if(dataLength > blockSize) {
          throw new IOException("Data size ("+ dataLength +" bytes) greater than specified block size(" + blockSize + " bytes)");
        }

        // write block trailer & reset variables
        if(dataLength > remainingBytes) {
          logger_.debug("Key : " + keyBytes + " with value doesnt fit in remaining "+ remainingBytes + " bytes.");        
          globalOffset = updateBlockTrailer(blockKeyOffset_, blockTrailerOffsets, globalOffset);        
          logger_.debug("Creating new block @ " + globalOffset);
          currentBlockOffset = 0;
          remainingBytes = blockSize;        
        }

        logger_.debug("write@ "+ globalOffset +" key: " + keyBytes + ""
            + " (hash: " + keyBytes.hashCode() + ")");      
        output_.writeInt(value.length);
        // write value (key can be retrieved from block trailer)
        output_.write(value);
        // store key-offset pair (needed for block trailer)
        blockKeyOffset_.put(keyBytes, currentBlockOffset);
        currentBlockOffset += (dataLength);
        remainingBytes -= dataLength;
      }

      // write the last block trailer information
      globalOffset = updateBlockTrailer(blockKeyOffset_, blockTrailerOffsets, globalOffset); 

      // write global trailer (block start offset-block trailer offset pair)
      output_.writeInt(blockTrailerOffsets.size());
      List<Integer> allBlockKeys = new ArrayList<>(blockTrailerOffsets.keySet());
      Collections.sort(allBlockKeys);
      for(int blockStart : allBlockKeys) {
        output_.writeInt(blockStart);
        output_.writeInt(blockTrailerOffsets.get(blockStart));
      }
      raf.close();
      output_.flush();
      output_.close();
      
      // fill in the previously created placeholder for trailer offset
      raf = new RandomAccessFile(mapFile_, "rw");   
      raf.seek(12);
      logger_.info("DMap Trailer start at " + globalOffset + ".");
      raf.writeInt(globalOffset);                
    } catch(IOException ioe) {
      throw ioe;
    } finally {
      // delete the intermediate temp file
      tmpMapFile_.delete();
      raf.close();
    }
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