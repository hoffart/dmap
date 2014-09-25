package de.jhoff.dmap;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import de.jhoff.dmap.util.ByteUtils;


public class DMapTest {

  @Test
  public void testDMapWithoutPreloadingOffsets() throws IOException {
    File tmpFile = File.createTempFile("tmp", ".dmap");
    tmpFile.delete();

    DMapBuilder dmapBuilder = new DMapBuilder(tmpFile, 256);
    int count = 1 << 10;
    for (int i = 0; i < count; ++i) {
      dmapBuilder.add(ByteUtils.getBytes(i), ByteUtils.getBytes(i));
    }
    dmapBuilder.build();

    DMap dmap = new DMap.Loader(tmpFile).load();
    for (int i = 0; i < count; ++i) {
      byte[] value = dmap.get(ByteUtils.getBytes(i));
      assertEquals(i, ByteBuffer.wrap(value).getInt());
    }
    assertEquals(null, dmap.get(ByteUtils.getBytes(count + 1)));
    assertEquals(null, dmap.get(ByteUtils.getBytes(-1)));
    tmpFile.delete();
  }

  @Test
  public void testDMapWithOffsetsPreloading() throws IOException {
    File tmpFile = File.createTempFile("tmp", ".dmap");
    tmpFile.delete();

    DMapBuilder dmapBuilder = new DMapBuilder(tmpFile, 256);
    int count = 1 << 10;
    for (int i = 0; i < count; ++i) {
      dmapBuilder.add(ByteUtils.getBytes(i), ByteUtils.getBytes(i));
    }
    dmapBuilder.build();

    DMap dmap = new DMap.Loader(tmpFile)
                        .preloadOffsets()
                        .load();
    for (int i = 0; i < count; ++i) {
      byte[] value = dmap.get(ByteUtils.getBytes(i));
      assertEquals(i, ByteBuffer.wrap(value).getInt());
    }
    assertEquals(null, dmap.get(ByteUtils.getBytes(count + 1)));
    assertEquals(null, dmap.get(ByteUtils.getBytes(-1)));
    tmpFile.delete();
  }

  @Test
  public void randomTestWithOffsetPreloading() throws IOException {
    File tmpFile = File.createTempFile("tmp", ".dmap");
    tmpFile.delete();

    DMapBuilder dmapBuilder = new DMapBuilder(tmpFile, 256);
    int count = 1 << 10;
    Random r = new Random();
    Map<Integer, Integer> kvs = new HashMap<>();
    for (int i = 0; i < count; ++i) {
      int k = r.nextInt();
      int v = r.nextInt();
      kvs.put(k, v);
      dmapBuilder.add(ByteUtils.getBytes(k), ByteUtils.getBytes(v));
    }
    dmapBuilder.build();

    DMap dmap = new DMap.Loader(tmpFile)
                        .preloadOffsets()
                        .load();
    for (Entry<Integer, Integer> e : kvs.entrySet()) {
      byte[] value = dmap.get(ByteUtils.getBytes(e.getKey()));
      assertEquals(e.getValue().intValue(), ByteBuffer.wrap(value).getInt());
    }    
    assertEquals(null, dmap.get(ByteUtils.getBytes(count + 1)));
    assertEquals(null, dmap.get(ByteUtils.getBytes(-1)));
    tmpFile.delete();
  }

  @Test
  public void randomTestWithoutOffsetPreloading() throws IOException {
    File tmpFile = File.createTempFile("tmp", ".dmap");
    tmpFile.delete();

    DMapBuilder dmapBuilder = new DMapBuilder(tmpFile, 256);
    int count = 1 << 10;
    Random r = new Random();
    Map<Integer, Integer> kvs = new HashMap<>();
    for (int i = 0; i < count; ++i) {
      int k = r.nextInt();
      int v = r.nextInt();
      kvs.put(k, v);
      dmapBuilder.add(ByteUtils.getBytes(k), ByteUtils.getBytes(v));
    }
    dmapBuilder.build();

    DMap dmap = new DMap.Loader(tmpFile).load();
    for (Entry<Integer, Integer> e : kvs.entrySet()) {
      byte[] value = dmap.get(ByteUtils.getBytes(e.getKey()));
      assertEquals(e.getValue().intValue(), ByteBuffer.wrap(value).getInt());
    }
    assertEquals(null, dmap.get(ByteUtils.getBytes(count + 1)));
    assertEquals(null, dmap.get(ByteUtils.getBytes(-1)));
    tmpFile.delete();
  }

  @Test
  public void multiThreadTestWithOffsetPreloading() throws IOException, InterruptedException, ExecutionException {
    File tmpFile = File.createTempFile("tmp", ".dmap");
    tmpFile.delete();

    DMapBuilder dmapBuilder = new DMapBuilder(tmpFile, 256);
    int count = 1 << 10;
    Random r = new Random();
    Map<Integer, Integer> kvs = new HashMap<>();    
    for (int i = 0; i < count; ++i) {
      int k = r.nextInt();
      int v = r.nextInt();
      kvs.put(k, v);
      dmapBuilder.add(ByteUtils.getBytes(k), ByteUtils.getBytes(v));
    }
    dmapBuilder.build();

    int threadCount = 100;
    ExecutorService es = Executors.newFixedThreadPool(threadCount);
    List<Future<Boolean>> results = new ArrayList<>(100);
    DMap dmap = new DMap.Loader(tmpFile)
                        .preloadOffsets()
                        .load();
    for (int t = 0; t < threadCount; ++t) {
      Reader reader = new Reader(dmap, kvs);
      Future<Boolean> result = es.submit(reader);
      results.add(result);
    }
    for (Future<Boolean> result : results) {
      assertTrue(result.get());
    }
    tmpFile.delete();
  }

  @Test
  public void multiThreadTestWithoutOffsetPreloading() throws IOException, InterruptedException, ExecutionException {
    File tmpFile = File.createTempFile("tmp", ".dmap");
    tmpFile.delete();

    DMapBuilder dmapBuilder = new DMapBuilder(tmpFile, 256);
    int count = 1 << 10;
    Random r = new Random();
    Map<Integer, Integer> kvs = new HashMap<>();
    for (int i = 0; i < count; ++i) {
      int k = r.nextInt();
      int v = r.nextInt();
      kvs.put(k, v);
      dmapBuilder.add(ByteUtils.getBytes(k), ByteUtils.getBytes(v));
    }
    dmapBuilder.build();

    int threadCount = 100;
    ExecutorService es = Executors.newFixedThreadPool(threadCount);
    List<Future<Boolean>> results = new ArrayList<>(100);
    DMap dmap = new DMap.Loader(tmpFile).load();
    for (int t = 0; t < threadCount; ++t) {
      Reader reader = new Reader(dmap, kvs);
      Future<Boolean> result = es.submit(reader);
      results.add(result);
    }
    for (Future<Boolean> result : results) {
      assertTrue(result.get());
    }
    tmpFile.delete();
  }

  private class Reader implements Callable<Boolean> {
    private DMap dmap_;
    private Map<Integer, Integer> toRead_;
    
    private Reader(DMap dmap, Map<Integer, Integer> toRead) {
      dmap_ = dmap;
      toRead_ = toRead;
    }

    @Override
    public Boolean call() throws Exception {
      for (Entry<Integer, Integer> e : toRead_.entrySet()) {
        byte[] value = dmap_.get(ByteUtils.getBytes(e.getKey()));
        if (e.getValue().intValue() != ByteBuffer.wrap(value).getInt()) {
          return false;
        }
      }
      // Everything was correct.
      return true;
    }
  }
}