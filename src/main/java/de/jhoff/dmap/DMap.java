package de.jhoff.dmap;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.jhoff.dmap.util.ByteArray;
import de.jhoff.dmap.util.ByteArrayUtils;
import de.jhoff.dmap.util.map.CachingHashMap;

/**
 * Disk-backed implementation of a very simple Map that supports only
 */
public class DMap {
  private static final int DMAP_VERSION = 1;

  private static final int DEFAULT_BLOCK_LIMIT = 3;

  /** Current Map file generated by Builder has Global trailer offset at 12. */
  private static final int DEFAULT_LOC_FOR_TRAILER_OFFSET = 12;

  /** Map file with data. */
  private final File mapFile_;
  private final RandomAccessFile raf_;

  /** Number of entries in the map. */
  private final int size;

  /** Maximum number of blocks that can be used by dmap at a time. */
  private final int maxBlockLimit_;

  /** First Key - Mapped block pair. */
  private Map<ByteArray, MappedByteBuffer>  cachedByteBuffers_;

  /** Mapping of first key of block to block's start offset. */
  private final Map<ByteArray, Integer> firstKeyInBlock_;

  /** Mapping of block start offset to block trailer offset. */
  private final Map<Integer, Integer> blockOffsetInfo_;

  /** Flag to enable/disable preloading of key offset pairs. */
  private final boolean preloadAllKeyOffsets;

  /** Flag to enable/disable preloading of all the values. */
  private final boolean preloadAllValues;

  /** Mapping of BlockTrailer Start offset and block trailer mapped bytebuffer of trailer. */
  private final Map<Integer, MappedByteBuffer> blockTrailerBuffer;

  /** Mapping of BlockTrailer start offset and all key-offset pairs info contained in the trailer. */
  private final Map<Integer, Map<ByteArray, Integer>> blockTrailerKeys;

  /** First keys of all the blocks present in the dmap loaded once. */
  private ByteArray[] firstKeys;

  private final Logger logger_ = LoggerFactory.getLogger(DMap.class);

  private DMap(Builder loader) throws IOException {
    mapFile_ = loader.mapFile_;
    preloadAllKeyOffsets = loader.preloadOffsets_;
    preloadAllValues = loader.preloadValues_;
    maxBlockLimit_ = loader.maxBlockLimit_;

    cachedByteBuffers_ = new CachingHashMap<>(maxBlockLimit_);
    raf_ = new RandomAccessFile(mapFile_, "r");

    int version = raf_.readInt();
    if(version != DMAP_VERSION) {
      throw new IOException("Invalid version of DMap file encountered. Please fix.");
    }

    // Sorted first keys
    firstKeyInBlock_ = new TreeMap<>();
    blockOffsetInfo_ = new HashMap<>();
    blockTrailerBuffer = new HashMap<>();
    blockTrailerKeys = new HashMap<>();

    size = raf_.readInt();

    loadKeyDetails();

    if (preloadAllValues) {
      int numBlocks = getBlockCount();
      // override the maxBlockLimit_
      cachedByteBuffers_ = new CachingHashMap<>(numBlocks);
      for(ByteArray firstKey : firstKeyInBlock_.keySet()) {
        int blockStart = firstKeyInBlock_.get(firstKey);
        int blockTrailerStart = firstKeyInBlock_.get(blockStart);
        FileChannel fc = raf_.getChannel();
        MappedByteBuffer mappedBuffer_ = fc.map(MapMode.READ_ONLY, blockStart, blockTrailerStart - blockStart);
        mappedBuffer_.load();
        cachedByteBuffers_.put(firstKey, mappedBuffer_);
      }
      logger_.debug("Preloaded all " + numBlocks + " blocks.");
    }

  }

  /*  This public Builder class allows creation of customized DMap instance.
   *  This is the Only way to create a DMap instance.
   */
  public static class Builder {
    private boolean preloadOffsets_;
    private boolean preloadValues_;
    private int maxBlockLimit_;
    private final File mapFile_;

    /**
     * A Loader constructor that takes a File parameter to be loaded into DMap.
     *
     * @param mapFile A File instance to be loaded.
     */
    public Builder(File mapFile) {
      mapFile_ = mapFile;
      maxBlockLimit_ = DEFAULT_BLOCK_LIMIT;
      // by default, both keyoffset loading and value loading will be disabled
      preloadOffsets_ = false;
      preloadValues_ = false;
    }

    /**
     * This method enables key-offset preloading during DMap instantiation
     *
     * @return The current Loader instance.
     */
    public Builder preloadOffsets() {
      this.preloadOffsets_ = true;
      return this;
    }

    /**
     * This method enables values preloading during DMap instantiation
     *
     * @return The current Loader instance.
     */
    public Builder preloadValues() {
      this.preloadValues_ = true;
      return this;
    }

    /**
     * This method sets the DMap block limit to specified value
     *
     * @return The current Loader instance.
     */
    public Builder setMaxBlockLimit(int value) {
      this.maxBlockLimit_ = value;
      return this;
    }

    /**
     * The parameter-less build method creates an instance of DMap.
     * This method needs to be called once all DMap customizations are done.
     *
     * @return A DMap instance.
     * @throws IOException
     */
    public DMap build() throws IOException {
      return new DMap(this);
    }
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
   * Get number of blocks in the map.
   * 
   * @return Number of blocks in the map.
   * @throws IOException
   */
  public int getBlockCount() throws IOException {
    int trailerOffset = getGlobalTrailerOffet();
    raf_.seek(trailerOffset);
    return raf_.readInt();
  }

  /**
   * Get byte[] value for key.
   * 
   * @param key Key to retrieve the value for.
   * @return  byte[] associated with key.
   */
  public byte[] get(byte[] key) throws IOException {
    ByteArray keyBytes = new ByteArray(key);
    logger_.debug("get(" + keyBytes + ") - hash: " + keyBytes.hashCode());
    // identify the block containing the given key using first key information.
    ByteArray firstKeyBytes = ByteArrayUtils.findMaxElementLessThanTarget(firstKeys, keyBytes);

    if(firstKeyBytes == null) {
      // key not in range (less than start key)
      return null;
    }

    MappedByteBuffer mappedBuffer_;
    int blockStart = firstKeyInBlock_.get(firstKeyBytes);
    int blockTrailerStart = blockOffsetInfo_.get(blockStart);
    // load the value offset
    Integer valueOffset = getValueOffset(keyBytes, blockTrailerStart);
    if (valueOffset == null) {
      return null;
    }

    mappedBuffer_ = cachedByteBuffers_.get(firstKeyBytes);
    if(mappedBuffer_ == null) {
      FileChannel fc = raf_.getChannel();
      mappedBuffer_ = fc.map(MapMode.READ_ONLY, blockStart, blockTrailerStart - blockStart);
      cachedByteBuffers_.put(firstKeyBytes, mappedBuffer_);
    }

    int valueLength = mappedBuffer_.getInt(valueOffset);
    byte[] value = new byte[valueLength];
    valueOffset += 4;
    for (int i = 0; i < value.length; ++i) {
      value[i] = mappedBuffer_.get(valueOffset);
      ++valueOffset;
    }
    return value;
  }

  /* NOTE:
   *
   *    When Offset preloading is disabled, this method does a linear search over all the keys in the given block
   * to find the matching key and retrieve the value offset associated with the key.
   *    Searching single block DMap contaning N keys will be slower than Searching M-Blocks DMap with each block containing
   *    a subset of key.
   */
  private Integer getValueOffset(ByteArray keyBytes, int blockTrailerStartOffset) throws IOException {
    int valueOffset = -1;
    if(!preloadAllKeyOffsets) {
      // time for linear search over the keys in block using mappedTrailer
      MappedByteBuffer mappedTrailerBuffer_ = blockTrailerBuffer.get(blockTrailerStartOffset);
      int pos = 0;
      // load key count - int
      int numKeysInBlock = mappedTrailerBuffer_.getInt(pos);
      pos+=4;

      // start search over keys
      for(int count=0; count<numKeysInBlock; count++) {
        int keyLen = mappedTrailerBuffer_.getInt(pos);
        pos+=4;
        byte[] currentkey = new byte[keyLen];
        for(int byteCount=0;byteCount<keyLen;byteCount++) {
          // increment by 1 since individual bytes are read
          currentkey[byteCount] = mappedTrailerBuffer_.get(pos++);
        }
        ByteArray currentKeyBytes = new ByteArray(currentkey);
        int offset = mappedTrailerBuffer_.getInt(pos);
        // logger_.debug("Comparing " + keyBytes + " and " + currentKeyBytes + " : " + keyBytes.compareTo(currentKeyBytes));
        if(keyBytes.compareTo(currentKeyBytes) == 0) {
          valueOffset = offset;
          break;
        }
        pos+=4;
      }
    } else {
      // just look up in the existing map
      Map<ByteArray, Integer> tmpMap = blockTrailerKeys.get(blockTrailerStartOffset);
      if(tmpMap != null && tmpMap.containsKey(keyBytes)) {
        valueOffset = tmpMap.get(keyBytes);
      }
    }

    // key doesnt exist in the block
    if(valueOffset == -1) {
      return null;
    }
    return valueOffset;
  }

  private int getGlobalTrailerOffet() throws IOException {
    raf_.seek(DEFAULT_LOC_FOR_TRAILER_OFFSET);
    return raf_.readInt();
  }

  private void processBlockTrailer(int trailerStartOffset, int trailerSize) throws IOException {
    FileChannel fc = raf_.getChannel();
    MappedByteBuffer trailerBuffer = fc.map(MapMode.READ_ONLY, trailerStartOffset, trailerSize);
    if(!preloadAllKeyOffsets) {
      blockTrailerBuffer.put(trailerStartOffset, trailerBuffer);
    } else {
      int numKeysInBlock = trailerBuffer.getInt();
      Map<ByteArray, Integer> tmpKeyOffsetMap = new HashMap<>();
      for(int count=0; count<numKeysInBlock; count++) {
        int keyLen = trailerBuffer.getInt();
        byte[] currentkey = new byte[keyLen];
        trailerBuffer.get(currentkey);
        ByteArray currentKeyBytes = new ByteArray(currentkey);
        int offset = trailerBuffer.getInt();
        tmpKeyOffsetMap.put(currentKeyBytes, offset);
      }
      blockTrailerKeys.put(trailerStartOffset, tmpKeyOffsetMap);
    }
  }

  private void loadKeyDetails() throws IOException {
    int numBlocks = getBlockCount();
    logger_.info("Number of blocks in file : " + numBlocks);
    int blockStart;
    int blockTrailerStart;
    int prevBlockTrailerStart = -1;

    for(int blockCount = 0; blockCount < numBlocks; ++blockCount) {
      blockStart = raf_.readInt();
      blockTrailerStart = raf_.readInt();
      int firstKeySize = raf_.readInt();
      byte[] firstKeyBytes = new byte[firstKeySize];
      raf_.read(firstKeyBytes);
      firstKeyInBlock_.put(new ByteArray(firstKeyBytes), blockStart);
      blockOffsetInfo_.put(blockStart, blockTrailerStart);
      if(blockCount > 0) {
        // compute the previous block's trailer size and store the information
        processBlockTrailer(prevBlockTrailerStart, blockStart - prevBlockTrailerStart);
      }
      prevBlockTrailerStart = blockTrailerStart;
    }
    processBlockTrailer(prevBlockTrailerStart, getGlobalTrailerOffet()-prevBlockTrailerStart);

    // load all the first keys for binary search during get()
    firstKeys = new ByteArray[firstKeyInBlock_.size()];
    firstKeyInBlock_.keySet().toArray(firstKeys);
  }
}