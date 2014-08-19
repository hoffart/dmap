package de.jhoff.dmap;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.junit.Test;

import de.jhoff.dmap.util.ByteUtils;


public class DMapBuilderTest {
  @Test
  public void test() throws IOException {
    File tmpFile = File.createTempFile("tmp", ".dmap");
    tmpFile.delete();
        
    DMapBuilder dmapBuilder = new DMapBuilder(tmpFile, 256);
    int count = 1 << 8;
    int version = 1;
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
    assertEquals(5168, raf.readInt());
    raf.seek(5168);
    assertEquals(8, raf.readInt());
    tmpFile.delete();
    raf.close();
  }
  
  @Test
  public void NoUnusedBlockSpaceTest() throws IOException {
    File tmpFile = File.createTempFile("tmp", ".dmap");
    tmpFile.delete();
        
    DMapBuilder dmapBuilder = new DMapBuilder(tmpFile, 10);
    int count = 2;
    for (int i = 0; i < count; ++i) {
      dmapBuilder.add(ByteUtils.getBytes(i), ByteUtils.getBytes(i));
    }     
    try{
      dmapBuilder.build();     
    }catch(IOException ioe){
      
    }
    
    RandomAccessFile raf = new RandomAccessFile(tmpFile, "r");            
    // trailer
    raf.seek(64);
    // total blocks
    assertEquals(2, raf.readInt());
    // start of first block
    assertEquals(16, raf.readInt());
    // start of first blocks trailer (should not be 26 for given block size 10)
    assertEquals(24, raf.readInt());
    // start of 2nd block
    assertEquals(40, raf.readInt());
    // start of 2nd blocks trailer
    assertEquals(48, raf.readInt());
    tmpFile.delete();
    raf.close();
  }
  
  @Test
  public void exceedingBlockSizetest() throws IOException {
    File tmpFile = File.createTempFile("tmp", ".dmap");
    tmpFile.delete();
        
    DMapBuilder dmapBuilder = new DMapBuilder(tmpFile,7);
    int count = 2;
    for (int i = 0; i < count; ++i) {
      dmapBuilder.add(ByteUtils.getBytes(i), ByteUtils.getBytes(i));
    }     
    try{
      dmapBuilder.build();     
    }catch(IOException ioe){
      
    }
    
    RandomAccessFile raf = new RandomAccessFile(tmpFile, "r");            
    tmpFile.delete();
    raf.close();
  }
}
