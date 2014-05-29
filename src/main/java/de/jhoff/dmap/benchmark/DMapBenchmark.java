package de.jhoff.dmap.benchmark;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import de.jhoff.dmap.DMap;
import de.jhoff.dmap.DMapBuilder;


public class DMapBenchmark {
  public static void main(String[] args) throws IOException {
    File mapFile = File.createTempFile("tmp", "dmap");
    mapFile.delete();

    DMapBuilder dmapBuilder = new DMapBuilder(mapFile);
    int values = 1 << 20;
    ByteBuffer buf = ByteBuffer.allocate(4);
    long time1 = System.currentTimeMillis();
    for (int i = 0; i < values; ++i) {
      byte[] bytes = buf.putInt(i).array();
      buf.rewind(); 
      dmapBuilder.add(bytes, bytes);
    }
    long time2 = System.currentTimeMillis();
    long runTime = time2 - time1;
    System.out.println("Added " + values + " (int,int) pairs in " 
        + runTime + "ms.");
    long time3 = System.currentTimeMillis();
    
    DMap dmap = new DMap(mapFile);    
    long time4 = System.currentTimeMillis();
    runTime = time4 - time3;
    System.out.println("Read map in " + runTime + "ms.");
    
    long time5 = System.currentTimeMillis();    
    int readCount = 1 << 20;
    Random r = new Random();
    for (int i = 0; i < readCount; ++i) {
      int keyInt = r.nextInt(values);
      byte[] key = buf.putInt(keyInt).array();
      buf.rewind();
      dmap.get(key);
    }
    long time6 = System.currentTimeMillis();
    runTime = time6 - time5;
    System.out.println("Random read of " + readCount + " keys took " + 
        runTime + "ms.");
        
    mapFile.delete();
  }
}
