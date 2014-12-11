package de.jhoff.dmap;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.*;

import de.jhoff.dmap.util.ExtendedFileChannel;
import gnu.trove.impl.Constants;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.jhoff.dmap.util.ByteArray;
import de.jhoff.dmap.util.ByteArrayUtils;
import de.jhoff.dmap.util.map.CachingHashMap;

/**
 * Disk-backed implementation of a very simple Map that supports only
 */
public class DMap {
  public static final int VERSION = 3;

  private static final int DEFAULT_BLOCK_CACHE_COUNT = 3;

  /** Current Map file generated by Builder has Global trailer offset at 12. */
  private static final int DEFAULT_LOC_FOR_TRAILER_OFFSET = 12;

  /** Map file with data. */
  private final File mapFile_;
  private final ExtendedFileChannel raf_;

  /** Number of entries in the map. */
  private final int size;

  /** Maximum number of blocks that can are held in memory when value blocks are held on disk. */
  private final int cacheBlockCount_;

  /** First Key - Mapped block pair. */
  private Map<ByteArray, MappedByteBuffer>  cachedByteBuffers_;

  /** Mapping of first key of block to block's start offset. */
  private final Map<ByteArray, Long> firstKeyInBlock_;

  /** Mapping of block start offset to block trailer offset. */
  private final Map<Long, Long> blockOffsetInfo_;

  /** Flag to enable/disable preloading of key offset pairs. */
  private final boolean preloadAllKeyOffsets;

  /** Flag to enable/disable preloading of all the values. */
  private final boolean preloadAllValues;
  
  /** Trove Map load factor (default: 0.5) */
  private final float troveLoadFactor = Constants.DEFAULT_LOAD_FACTOR;
  
  /** Trove Map no Entry value (default: -1) */
  private final int troveNoEntryValue = -1;

  /** Mapping of BlockTrailer Start offset and block trailer mapped bytebuffer of trailer. */
  private final Map<Long, MappedByteBuffer> blockTrailerBuffer_;

  /** Mapping of BlockTrailer start offset and all key-offset pairs info contained in the trailer. */
  private final Map<Long, TObjectIntHashMap<ByteArray>> blockTrailerKeys;

  /** First keys of all the blocks present in the dmap loaded once. */
  private ByteArray[] firstKeys;

  private final Logger logger_ = LoggerFactory.getLogger(DMap.class);

  private DMap(Builder loader) throws IOException {
    mapFile_ = loader.mapFile_;
    preloadAllKeyOffsets = loader.preloadOffsets_;
    preloadAllValues = loader.preloadValues_;
    cacheBlockCount_ = loader.cacheBlockSize_;

    cachedByteBuffers_ = new CachingHashMap<>(cacheBlockCount_);
    raf_ = new ExtendedFileChannel(new RandomAccessFile(mapFile_, "r").getChannel());

    int version = raf_.readInt();
    if(version != VERSION) {
      throw new IOException("Invalid version of DMap file encountered. Please fix.");
    }

    // Sorted first keys
    firstKeyInBlock_ = new TreeMap<>();
    blockOffsetInfo_ = new HashMap<>();
    blockTrailerBuffer_ = new HashMap<>();
    blockTrailerKeys = new HashMap<>();

    size = raf_.readInt();

    loadKeyDetails();

    if (preloadAllValues) {
      int numBlocks = getBlockCount();
      // override the cacheBlockCount_
      cachedByteBuffers_ = new CachingHashMap<>(numBlocks);
      for(ByteArray firstKey : firstKeyInBlock_.keySet()) {
        long blockStart = firstKeyInBlock_.get(firstKey);
        long blockTrailerStart = blockOffsetInfo_.get(blockStart);
        MappedByteBuffer mappedBuffer_ = raf_.map(MapMode.READ_ONLY, blockStart, blockTrailerStart - blockStart);
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
    private int cacheBlockSize_;
    private final File mapFile_;

    /**
     * A Loader constructor that takes a File parameter to be loaded into DMap.
     *
     * @param mapFile A File instance to be loaded.
     */
    public Builder(File mapFile) {
      mapFile_ = mapFile;
      cacheBlockSize_ = DEFAULT_BLOCK_CACHE_COUNT;
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
      this.cacheBlockSize_ = value;
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
  public synchronized int getBlockCount() throws IOException {
    long trailerOffset = getGlobalTrailerOffet();
    raf_.position(trailerOffset);
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

    MappedByteBuffer mappedBuffer;
    long blockStart = firstKeyInBlock_.get(firstKeyBytes);
    long blockTrailerStart = blockOffsetInfo_.get(blockStart);
    // load the value offset
    int valueOffset = getValueOffset(keyBytes, blockTrailerStart);
    if (valueOffset == troveNoEntryValue) {
      return null;
    }
    
    mappedBuffer = cachedByteBuffers_.get(firstKeyBytes);
    if(mappedBuffer == null) {
      mappedBuffer = raf_.map(MapMode.READ_ONLY, blockStart, blockTrailerStart - blockStart);
      cachedByteBuffers_.put(firstKeyBytes, mappedBuffer);
    }
    
    ByteBuffer slice = mappedBuffer.slice();
    slice.position(valueOffset);
    int valueLength = slice.getInt();
    byte[] value = new byte[valueLength];
    slice.get(value);
    return value;
  }

  /* NOTE:
   *
   *    When Offset preloading is disabled, this method does a linear search over all the keys in the given block
   * to find the matching key and retrieve the value offset associated with the key.
   *    Searching single block DMap contaning N keys will be slower than Searching M-Blocks DMap with each block containing
   *    a subset of key.
   */
  private int getValueOffset(ByteArray keyBytes, Long blockTrailerStartOffset) throws IOException {
    int valueOffset = troveNoEntryValue;
    if(!preloadAllKeyOffsets) {
      // time for linear search over the keys in block using mappedTrailer
      ByteBuffer trailerBuffer = blockTrailerBuffer_.get(blockTrailerStartOffset).slice();
      // load key count - int
      int numKeysInBlock = trailerBuffer.getInt();

      // start search over keys
      for(int count=0; count<numKeysInBlock; count++) {
        int keyLen = trailerBuffer.getInt();
        byte[] currentkey = new byte[keyLen];
        trailerBuffer.get(currentkey);
        ByteArray currentKeyBytes = new ByteArray(currentkey);
        int offset = trailerBuffer.getInt();
        // logger_.debug("Comparing " + keyBytes + " and " + currentKeyBytes + " : " + keyBytes.compareTo(currentKeyBytes));
        if(keyBytes.compareTo(currentKeyBytes) == 0) {
          valueOffset = offset;
          break;
        }
      }
    } else {
      // just look up in the existing map
      TObjectIntHashMap tmpMap = blockTrailerKeys.get(blockTrailerStartOffset);
      if(tmpMap != null) {
        valueOffset = tmpMap.get(keyBytes);
      }
    }
    return valueOffset;
  }

  private long getGlobalTrailerOffet() throws IOException {
    raf_.position(DEFAULT_LOC_FOR_TRAILER_OFFSET);
    return raf_.readLong();
  }

  private void processBlockTrailer(long trailerStartOffset, long trailerSize) throws IOException {
    MappedByteBuffer trailerBuffer = raf_.map(MapMode.READ_ONLY, trailerStartOffset, trailerSize);
    if(!preloadAllKeyOffsets) {
      blockTrailerBuffer_.put(trailerStartOffset, trailerBuffer);
    } else {
      int numKeysInBlock = trailerBuffer.getInt();
      TObjectIntHashMap<ByteArray> tmpKeyOffsetMap = 
        new TObjectIntHashMap<>((int) (numKeysInBlock/troveLoadFactor+0.5), troveLoadFactor, troveNoEntryValue);
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
    logger_.debug("Number of blocks in file : " + numBlocks);
    long blockStart;
    long blockTrailerStart;
    long prevBlockTrailerStart = -1;

    for(int blockCount = 0; blockCount < numBlocks; ++blockCount) {
      blockStart = raf_.readLong();
      blockTrailerStart = raf_.readLong();
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
  
  public EntryIterator entryIterator() {
    if (preloadAllKeyOffsets)
      return new EntryIteratorForPreloadedKeys();
    else 
      return new EntryIteratorWithoutPreloading();
  }

  private class EntryIteratorWithoutPreloading implements EntryIterator {
    Iterator<MappedByteBuffer> blockIterator_;
    ByteBuffer curBuffer_;
    int curBlockKeyNum_;
    int curKey_;
    
    private EntryIteratorWithoutPreloading() {
      blockIterator_ = blockTrailerBuffer_.values().iterator();
      if (blockIterator_.hasNext()) {
        curBuffer_ = blockIterator_.next().slice();
        curBlockKeyNum_ = curBuffer_.getInt();
      } else {
        curBuffer_ = null;
        curBlockKeyNum_ = 0;
      }
      curKey_ = 0;
    }

    @Override
    public boolean hasNext() {
      return curKey_ < curBlockKeyNum_ || blockIterator_.hasNext();
    }

    @Override
    public Entry next() throws IOException {
      // TODO: make it more efficient if necessary
      if (curKey_++ < curBlockKeyNum_) {
        int keyLen = curBuffer_.getInt();
        byte[] key = new byte[keyLen];
        curBuffer_.get(key);
        curBuffer_.position(curBuffer_.position() + 4);   // skip offset
        return new Entry(key, get(key));
      } else if (blockIterator_.hasNext()) {
        curBuffer_ = blockIterator_.next().slice();
        curBlockKeyNum_ = curBuffer_.getInt();
        curKey_ = 0;
        return next();
      } else 
        return null;
    }
  }

  private class EntryIteratorForPreloadedKeys implements EntryIterator {
    Iterator<TObjectIntHashMap<ByteArray>> blockIterator_;
    Iterator<ByteArray> keyIterator_;
    
    private EntryIteratorForPreloadedKeys() {
      blockIterator_ = blockTrailerKeys.values().iterator();
      if (blockIterator_.hasNext()) {
        keyIterator_ = blockIterator_.next().keySet().iterator();
      } else
        keyIterator_ = null;
    }

    @Override
    public boolean hasNext() {
      if (keyIterator_ != null && keyIterator_.hasNext())
        return true;
      else {
        while (blockIterator_.hasNext()) {
          keyIterator_ = blockIterator_.next().keySet().iterator();
          if (keyIterator_.hasNext())
            return true;
        }
      }
      return false;
    }

    @Override
    public Entry next() throws IOException {
      // TODO: make it more efficient if necessary
      if (keyIterator_.hasNext()) {
        ByteArray key = keyIterator_.next();
        return new Entry(key.getBytes(), get(key.getBytes()));
      }
      while (blockIterator_.hasNext()) {
        keyIterator_ = blockIterator_.next().keySet().iterator();
        if (keyIterator_.hasNext())
          return next();
      }
      return null;
    }
  }
  
  public static interface EntryIterator {
    public boolean hasNext();
    public Entry next() throws IOException;
  }
  
  public static class Entry {
    private byte[] key;
    private byte[] value;

    private Entry(byte[] key, byte[] value) {
      this.key = key;
      this.value = value;
    }

    public byte[] getKey() {
      return key;
    }

    public byte[] getValue() {
      return value;
    }
  }
}