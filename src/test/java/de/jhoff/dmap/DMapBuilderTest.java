package de.jhoff.dmap;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import de.jhoff.dmap.util.ByteUtils;


public class DMapBuilderTest {

  /** Used for testing scenario where data doesnt fit into block*/
  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testDMapBuilderForKeyValuePairs() throws IOException {
    File tmpFile = File.createTempFile("tmp", ".dmap");
    tmpFile.delete();
        
    DMapBuilder dmapBuilder = new DMapBuilder(tmpFile, 256);
    int count = 1 << 8;
    int version = 3;
    int defaultblockSize = 256;
    for (int i = 0; i < count; ++i) {
      dmapBuilder.add(ByteUtils.getBytes(i), ByteUtils.getBytes(i));
    }
    dmapBuilder.build();
   
    RandomAccessFile raf = new RandomAccessFile(tmpFile, "r");
    
    // header - version
    assertEquals(version, raf.readInt());
    // header - number of entries
    assertEquals(count, raf.readInt());
    // header - block size
    assertEquals(defaultblockSize, raf.readInt());
    // trailer starts at (16 + (totblock * (block size + block trailer size))
    assertEquals(5172, raf.readLong());
    raf.seek(5172);
    assertEquals(8, raf.readInt());
    tmpFile.delete();
    raf.close();
  }
  
  @Test
  public void testForNoUnusedBlockSpace() throws IOException {
    File tmpFile = File.createTempFile("tmp", ".dmap");
    tmpFile.delete();
        
    DMapBuilder dmapBuilder = new DMapBuilder(tmpFile, 10);
    int count = 2;
    for (int i = 0; i < count; ++i) {
      dmapBuilder.add(ByteUtils.getBytes(i), ByteUtils.getBytes(i));
    }     

    dmapBuilder.build();     

    RandomAccessFile raf = new RandomAccessFile(tmpFile, "r");
    // trailer
    raf.seek(68);
    // total blocks
    assertEquals(2, raf.readInt());
    // start of first block
    assertEquals(20, raf.readLong());
    // start of first blocks trailer (should not be 26 for given block size 10)
    assertEquals(28, raf.readLong());
    // next should be the length of the first key (integer)
    assertEquals(4, raf.readInt());
    // followed by the key itself (int again)
    assertEquals(0, raf.readInt());
    // start of 2nd block
    assertEquals(44, raf.readLong());
    // start of 2nd blocks trailer
    assertEquals(52, raf.readLong());
    tmpFile.delete();
    raf.close();
  }

  @Test
  public void testForDataExceedingBlockSizeThrowsException() throws IOException {
    File tmpFile = File.createTempFile("tmp", ".dmap");
    tmpFile.delete();
    // data consist of an int(4 bytes) and its length(4 bytes) which doesnt fit in a single block. Throw IOException and exit.
    DMapBuilder dmapBuilder = new DMapBuilder(tmpFile, 7);
    int count = 2;
    for (int i = 0; i < count; ++i) {
      dmapBuilder.add(ByteUtils.getBytes(i), ByteUtils.getBytes(i));
    }     

    exception.expect(IOException.class);
    dmapBuilder.build();
    tmpFile.delete();
  }

  @Test
  public void testForDuplicateKeyThrowsIOException() throws IOException {
    File tmpFile = File.createTempFile("tmp", ".dmap");
    tmpFile.delete();
    DMapBuilder dmapBuilder = new DMapBuilder(tmpFile, 10);
    int count = 2;
    for (int i = 0; i < count; ++i) {
      dmapBuilder.add(ByteUtils.getBytes(1), ByteUtils.getBytes(1));
    }     

    exception.expect(IOException.class);
    dmapBuilder.build();
    tmpFile.delete();
  }
  
  @Test
  public void testSortedKeysInMapFile() throws IOException {
    File tmpFile = File.createTempFile("tmp", ".dmap");
    tmpFile.delete();
        
    DMapBuilder dmapBuilder = new DMapBuilder(tmpFile, 20);
    int count = 10;
    for (int i = count; i > 0; --i) {
      dmapBuilder.add(ByteUtils.getBytes(i), ByteUtils.getBytes(i));
    }     

    dmapBuilder.build();     
    RandomAccessFile raf = new RandomAccessFile(tmpFile, "r");            
    // verify 3rd blocks offset info stored in global trailer (starts at 272)
    //TODO: fix for long
    raf.seek(292);
    assertEquals(108, raf.readLong());
    // start of 3rd blocks trailer
    assertEquals(124, raf.readLong());
    // the length of first key in 3rd block should be 4
    assertEquals(4, raf.readInt());
    // the first key itself should be 5.
    assertEquals(5, raf.readInt());
    // check for key-values in block 1
    raf.seek(24);
    assertEquals(1, raf.readInt());
    raf.seek(32);
    assertEquals(2, raf.readInt());

    // check for key-values in block 3
    raf.seek(112);
    assertEquals(5, raf.readInt());
    raf.seek(120);
    assertEquals(6, raf.readInt());

    // check for key-values in block 5
    raf.seek(200);
    assertEquals(9, raf.readInt());
    raf.seek(208);
    assertEquals(10, raf.readInt());

    tmpFile.delete();
    raf.close();
  }
}
