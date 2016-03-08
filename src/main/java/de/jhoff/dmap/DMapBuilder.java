package de.jhoff.dmap;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import de.jhoff.dmap.util.CompressionUtils;
import de.jhoff.dmap.util.ExtendedFileChannel;
import org.iq80.snappy.Snappy;
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

  /** Default Key-Value Block size (in bytes) - set to 1 MB. */
  private static final int DEFAULT_BLOCK_SIZE = 1048576;

  /** Current block size for the file*/
  private int blockSize_;
  
  /** Compress values */
  private boolean compressValues_;

  /** Map file to write to. */
  private File mapFile_;

  /** Writer to the map file. */
  private ExtendedFileChannel output_;

  /** Temporary Map file. */
  private File tmpMapFile_;

  /** Writer to the temp file. */
  private ExtendedFileChannel tmpOutput_;

  /** Keep track of number of entries retrieved from original file */
  private int entriesCount_;

  /** Keep track of number of bytes written */
  private int byteCount_;

  private final Logger logger_ = LoggerFactory.getLogger(DMapBuilder.class);

  public DMapBuilder(File mapFile) throws IOException {
    this(mapFile, DEFAULT_BLOCK_SIZE);
  }
  
  public DMapBuilder(File mapFile, int blockSize) throws IOException {
    this(mapFile, blockSize, true);
  }

  /**
   * 
   * @param mapFile Map File instance.
   * @param blockSize Size of a block (in bytes).
   * @throws IOException
   */
  public DMapBuilder(File mapFile, int blockSize, boolean compressValues) throws IOException {
    boolean success = mapFile.createNewFile();
    if (success) {
      blockSize_ = blockSize;
      compressValues_ = compressValues;
      mapFile_ = mapFile;
      try {
        tmpMapFile_ = File.createTempFile("tmpDMap_", "_" + mapFile.getName());
      } catch (Exception e) {
        throw new IOException("Error creating intermediate file: " + tmpMapFile_ + ", cannot write.");
      }
      tmpOutput_ = new ExtendedFileChannel(new RandomAccessFile(tmpMapFile_, "rw").getChannel());
      
      
      output_ = new ExtendedFileChannel(new RandomAccessFile(mapFile_, "rw").getChannel());
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
    byteCount_ += key.length + value.length;
  }

  public void build() throws IOException {
    Map<ByteArray, Long> tmpKeyOffsetMap =  new HashMap<>();
    tmpOutput_.flush();
    tmpOutput_.close();

    ExtendedFileChannel raf = new ExtendedFileChannel(new RandomAccessFile(tmpMapFile_, "r").getChannel());
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
        currentOffset = raf.position() + valLen;
        raf.position(currentOffset);
      }

      logger_.debug("Loaded " + tmpKeyOffsetMap.size() + " keys from temporary file");

      // global header - version, entries count, block size, trailer offset
      output_.writeInt(DMap.VERSION);
      output_.writeInt(tmpKeyOffsetMap.size());
      output_.writeInt(blockSize_);
      output_.writeBool(compressValues_);
      // insert placeholder for trailer offset
      output_.writeLong(0);

      List<ByteArray> allKeys = new ArrayList<>(tmpKeyOffsetMap.keySet());
      Collections.sort(allKeys);
      logger_.info("Map size: " + byteCount_ + " bytes ");
      logger_.info("Writing map for " + allKeys.size() + " keys.");

      long globalOffset = output_.position();
      int currentBlockOffset = 0;
      int remainingBytes = blockSize_;
      ByteArray firstKey = null;

      // Map to store block-level key-offset pairs (to be written to each block trailer)
      Map<ByteArray, Integer> blockKeyOffset_ = new HashMap<>();
      // Map to store blockStart-blockTrailerStart pair (to be written to global trailer)
      Map<Long, Long> blockTrailerOffsets = new HashMap<>();
      // Map to store blockStart-firstKey pair (to be written to global trailer)
      Map<Long, ByteArray> blockFirstKey = new HashMap<>();

      for (ByteArray keyBytes : allKeys) {
        long offset = tmpKeyOffsetMap.get(keyBytes);
        raf.position(offset);
        int keyLen = raf.readInt();
        int valLen = raf.readInt();
        value = new byte[valLen];
        // position pointer at the starting of value data
        raf.position(raf.position() + keyLen);
        raf.read(value);

        if (compressValues_) {
          value = Snappy.compress(value);
        }
        
        int dataLength = CompressionUtils.getVNumSize(value.length) + value.length;

        if(dataLength > blockSize_) {
          throw new IOException("Data size ("+ dataLength +" bytes) greater than specified block size(" + blockSize_ + " bytes)");
        }

        // write block trailer & reset variables
        if(dataLength > remainingBytes) {
          logger_.debug("Key : " + keyBytes + " with value doesnt fit in remaining "+ remainingBytes + " bytes.");
          globalOffset = updateBlockTrailer(blockKeyOffset_, blockTrailerOffsets, blockFirstKey, firstKey, globalOffset);
          logger_.debug("Creating new block @ " + globalOffset);
          currentBlockOffset = 0;
          remainingBytes = blockSize_;
          firstKey = null;
        }

        if(firstKey == null) {
          firstKey = keyBytes;
        }

        logger_.debug("write@ " + globalOffset + " key: " + keyBytes + ""
          + " (hash: " + keyBytes.hashCode() + ")");
        output_.writeVInt(value.length);
        // write value (key can be retrieved from block trailer)
        output_.write(value);
        // store key-offset pair (needed for block trailer)
        blockKeyOffset_.put(keyBytes, currentBlockOffset);
        currentBlockOffset += (dataLength);
        remainingBytes -= dataLength;
      }

      // write the last block trailer information
      globalOffset = updateBlockTrailer(blockKeyOffset_, blockTrailerOffsets, blockFirstKey, firstKey, globalOffset);

      // write global trailer (block start offset-block trailer offset pair & first key in the block)
      output_.writeVInt(blockTrailerOffsets.size());
      List<Long> allBlockKeys = new ArrayList<>(blockTrailerOffsets.keySet());
      Collections.sort(allBlockKeys);
      for(long blockStart : allBlockKeys) {
        output_.writeVLong(blockStart);
        output_.writeVLong(blockTrailerOffsets.get(blockStart));
        byte[] tmpFirstKeyByte = blockFirstKey.get(blockStart).getBytes();
        // write the first key info to global trailer
        output_.writeVInt(tmpFirstKeyByte.length);
        output_.write(tmpFirstKeyByte);
      }
      raf.close();
      output_.flush();
      output_.close();

      // fill in the previously created placeholder for trailer offset
      raf = new ExtendedFileChannel(new RandomAccessFile(mapFile_, "rw").getChannel());
      raf.position(DMap.DEFAULT_LOC_FOR_TRAILER_OFFSET);
      logger_.info("DMap Trailer start at " + globalOffset + ".");
      raf.writeLong(globalOffset);
    } finally {
      // delete the intermediate temp file
      tmpMapFile_.delete();
      raf.close();
    }
  }

  /*
   * Returns new global offset after updating the block trailer
   */
  private long updateBlockTrailer(Map<ByteArray, Integer> keyOffsets,
      Map<Long, Long> blockTrailerOffsets,
      Map<Long, ByteArray> blockFirstKey,
      ByteArray firstKey, long globalOffset) throws IOException {
    long trailerOffset = output_.size();
    // write number of entries in the current block
    output_.writeVInt(keyOffsets.size());
    for(Entry<ByteArray, Integer> e : keyOffsets.entrySet()) {
      ByteArray byteArray = e.getKey();
      output_.writeVInt(byteArray.getBytes().length);
      output_.write(byteArray.getBytes());
      output_.writeVInt(e.getValue());
    }
    keyOffsets.clear();
    // track block offset info
    blockTrailerOffsets.put(globalOffset, trailerOffset);
    // track first keys in each block
    blockFirstKey.put(globalOffset, firstKey);
    return output_.position();
  }
}