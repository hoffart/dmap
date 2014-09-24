package de.jhoff.dmap.benchmark;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import de.jhoff.dmap.DMap;
import de.jhoff.dmap.DMapBuilder;


public class DMapBenchmark {
  public static void main(String[] args) throws IOException {
    
    int[] arrBlockSizes = new int[] {1024, 1048576}; // 1 KB, 1 MB
    int[] arrKeys = new int[] {1<<10, 1<<15}; // number of keys to be used for benchmarking
    
    for(int bSize : arrBlockSizes) {
      for(int keyValue : arrKeys) {
        runDmapBenchmarkTest(bSize, keyValue);
      }
    }
    
  }
  
  private static void runDmapBenchmarkTest(int blockSize, int keys) throws IOException {
    File mapFile = File.createTempFile("tmp", "dmap");
    mapFile.delete();

    DMapBuilder dmapBuilder = new DMapBuilder(mapFile, blockSize);
    ByteBuffer buf = ByteBuffer.allocate(4);
    long time1 = System.currentTimeMillis();
    for (int i = 0; i < keys; ++i) {
      byte[] bytes = buf.putInt(i).array();
      buf.rewind(); 
      dmapBuilder.add(bytes, bytes);
    }
    dmapBuilder.build();
    long time2 = System.currentTimeMillis();
    long runTime = time2 - time1;
    System.out.println("Added " + keys + " (int,int) pairs in " 
        + runTime + "ms.");
    long time3 = System.currentTimeMillis();
    
    DMap dmap = new DMap(mapFile);    
    long time4 = System.currentTimeMillis();
    runTime = time4 - time3;
    System.out.println("Read map in " + runTime + "ms.");
    
    long time5 = System.currentTimeMillis();    
    Random r = new Random();
    for (int i = 0; i < keys; ++i) {
      int keyInt = r.nextInt(keys);
      byte[] key = buf.putInt(keyInt).array();
      buf.rewind();
      dmap.get(key);
    }
    long time6 = System.currentTimeMillis();
    runTime = time6 - time5;
    System.out.println("Random read of " + keys + " keys with block size : "+ blockSize +" took " + 
        runTime + "ms.");
        
    mapFile.delete();
  }
}
