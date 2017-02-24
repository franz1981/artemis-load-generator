/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.load.generator;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import net.openhft.affinity.AffinityLock;
import org.HdrHistogram.Histogram;
import org.agrona.BufferUtil;
import org.agrona.IoUtil;
import org.agrona.UnsafeAccess;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;

public class SingleWriterBatchingBenchmark {

   //HINT: needs to be a realistic value <-> a power of 2!
   private static final int OS_PAGE_SIZE = Integer.getInteger("os.page.size", 4096);

   public static void main(String[] args) {
      int writersParam = 1;
      int pageBytesParam = OS_PAGE_SIZE;
      int iterationsParam = 20_000;
      int runsParam = 5;
      int bufferBytesParam = OS_PAGE_SIZE * 1024 * 16;
      boolean askedForHelp = false;
      boolean presetLengthParam = true;
      boolean fillLengthParam = true;
      String outParam = System.getProperty("java.io.tmpdir");

      for (int i = 0; i < args.length; ++i) {
         final String arg = args[i];
         switch (arg) {
            case "--writers":
               writersParam = Integer.parseInt(args[++i]);
               break;
            case "--page-bytes":
               pageBytesParam = Integer.parseInt(args[++i]);
               break;
            case "--buffer-bytes":
               bufferBytesParam = Integer.parseInt(args[++i]);
               break;
            case "--iterations":
               iterationsParam = Integer.parseInt(args[++i]);
               break;
            case "--runs":
               runsParam = Integer.parseInt(args[++i]);
               break;
            case "--not-preset-length":
               presetLengthParam = false;
               break;
            case "--not-fill-length":
               fillLengthParam = false;
               break;
            case "--out":
               outParam = args[++i];
               break;
            case "--help":
               askedForHelp = true;
               break;
            default:
               throw new AssertionError("Invalid args: " + args[i] + " try throughputBenchmark --help");
         }
      }
      if (askedForHelp) {
         final String validArgs = "\"[--writers writers] [--page-bytes pageBytes] [--runs runs] [--iterations totalWritesPerRun] [--flush flushFrequency] [--not-preset-length] [--not-fill-length] [--out outputFolder]\"";
         System.err.println("valid arguments = " + validArgs);
         if (args.length == 1) {
            return;
         }
      }

      final int writers = writersParam;
      final int pageBytes = pageBytesParam;
      final int bufferBytes = bufferBytesParam;
      final int ringbufferSize = RingBufferDescriptor.TRAILER_LENGTH + bufferBytes;
      RingBufferDescriptor.checkCapacity(ringbufferSize);
      final int iterations = iterationsParam;
      if (iterations == 0) {
         throw new IllegalArgumentException("iterations must be>0!");
      }
      final long expectedLength = ((long) iterations * pageBytes * writers);
      if (expectedLength > (Integer.MAX_VALUE)) {
         throw new IllegalArgumentException("the mapped option doesn't support expectedLength > " + expectedLength);
      }
      final int runs = runsParam;
      final boolean presetLength = presetLengthParam;
      final boolean fillLength = fillLengthParam;
      final File outFolder = new File(outParam);
      final AtomicInteger barrier = new AtomicInteger();
      final Thread[] writerThreads = new Thread[writers];
      final Object outputLock = new Object();

      //configuration summary
      System.out.println("***********\tCONFIGURATION SUMMARY:\t***********");
      System.out.println("writers: " + writers);
      System.out.println("runs: " + runs);
      System.out.println("iterations: " + iterations);
      System.out.println("pageBytes: " + pageBytes + " bytes");
      System.out.println("bufferBytes: " + bufferBytes + " bytes");
      System.out.println("presetLength: " + presetLength);
      System.out.println("fillLength: " + fillLength);
      System.out.println("temp file expected length: " + expectedLength + " bytes");
      System.out.println("outFolder: " + outFolder);
      System.out.println("histograms could be plotted on http://hdrhistogram.github.io/HdrHistogram/plotFiles.html");
      System.out.println("***********\tEND CONFIGURATION SUMMARY:\t***********");

      final String tmpDir = System.getProperty("java.io.tmpdir");
      final File ringBufferFile = new File(tmpDir, UUID.randomUUID().toString());
      ringBufferFile.deleteOnExit();
      final MappedByteBuffer mappedRingBuffer = IoUtil.mapNewFile(ringBufferFile, ringbufferSize);
      final UnsafeBuffer ringBufferBytes = new UnsafeBuffer(mappedRingBuffer);
      ringBufferBytes.setMemory(0, ringbufferSize, (byte) 0);
      final ManyToOneRingBuffer ringBuffer = new ManyToOneRingBuffer(new UnsafeBuffer(IoUtil.mapNewFile(ringBufferFile, ringbufferSize)));
      final Thread appenderThread = new Thread(new Runnable() {
         @Override
         public void run() {
            final Histogram batchSizeHistogram = new Histogram(bufferBytes, 0);
            final Histogram flushHistogram = new Histogram(TimeUnit.SECONDS.toNanos(10), 2);
            try (AffinityLock affinityLock = AffinityLock.acquireLock()) {
               final long writerIterations = iterations * writers;
               for (int r = 0; r < runs; r++) {
                  try {
                     final File file = File.createTempFile("temp", ".journal", outFolder);
                     file.deleteOnExit();
                     final MappedByteBuffer mappedBytes;
                     try (final RandomAccessFile raf = new RandomAccessFile(file, "rw"); final FileChannel fc = raf.getChannel()) {
                        mappedBytes = fc.map(FileChannel.MapMode.READ_WRITE, 0, expectedLength);
                     }
                     if (fillLength) {
                        UnsafeAccess.UNSAFE.setMemory(null,BufferUtil.address(mappedBytes),expectedLength,(byte)0);
                        mappedBytes.force();
                     }
                     batchSizeHistogram.reset();
                     flushHistogram.reset();
                     //wait to start!
                     barrier.incrementAndGet();
                     while (barrier.get() != (writers + 1)) {
                        LockSupport.parkNanos(1L);
                     }

                     final long elapsed;
                     try {
                        final long start = System.nanoTime();
                        long iterations = writerIterations;
                        while (iterations > 0) {
                           final int remaining = (int) Math.min(iterations, Integer.MAX_VALUE);
                           final int read = ringBuffer.read((msgTypeId, buffer, index, length) -> {
                              buffer.getBytes(index, mappedBytes, length);
                           }, remaining);
                           batchSizeHistogram.recordValue(read * pageBytes);
                           if (read > 0) {
                              iterations -= read;
                              //flush!
                              final long startFlush = System.nanoTime();
                              mappedBytes.force();
                              flushHistogram.recordValue(System.nanoTime() - startFlush);
                           }
                        }
                        elapsed = System.nanoTime() - start;
                     }
                     finally {
                        //wait to finish!
                        barrier.decrementAndGet();
                        while (barrier.get() != 0) {
                           LockSupport.parkNanos(1L);
                        }
                        //unmap and delete
                        IoUtil.unmap(mappedBytes);
                        file.delete();
                     }
                     synchronized (outputLock) {
                        System.out.println("**********\tAPPENDER RUN " + (r + 1) + " OF [" + Thread.currentThread().getId() + "]\t**********");
                        System.out.println(((expectedLength * 1000_000_000L) / elapsed) + " bytes/sec");
                        System.out.println("**********\tBATCH_SIZE (bytes)\t**********");
                        batchSizeHistogram.outputPercentileDistribution(System.out, 1.0);
                        System.out.println("**********\tFLUSH (us)\t**********");
                        flushHistogram.outputPercentileDistribution(System.out, 1000.0);
                        System.out.println("**********\tEND APPENDER RUN " + (r + 1) + " OF [" + Thread.currentThread().getId() + "]\t**********");
                     }
                  }
                  catch (IOException ie) {
                     throw new IllegalStateException(ie);
                  }
               }
            }
         }
      });
      appenderThread.start();
      for (int w = 0; w < writers; w++) {
         final Thread writerThread = new Thread(() -> {
            try (AffinityLock affinityLock = AffinityLock.acquireLock()) {

               final UnsafeBuffer pageBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(pageBytes).order(ByteOrder.nativeOrder()));
               final Histogram writeHistogram = new Histogram(TimeUnit.SECONDS.toNanos(10), 2);
               final Histogram waitHistogram = new Histogram(TimeUnit.SECONDS.toNanos(10), 2);
               for (int r = 0; r < runs; r++) {
                  writeHistogram.reset();
                  waitHistogram.reset();
                  final long elapsedRun;
                  barrier.incrementAndGet();
                  while (barrier.get() != (writers + 1)) {
                     LockSupport.parkNanos(1L);
                  }

                  final long startRun = System.nanoTime();
                  for (int it = 0; it < iterations; it++) {
                     final long startWrite = System.nanoTime();
                     long lastWait = startWrite;
                     while (!ringBuffer.write(1, pageBuffer, 0, pageBytes)) {
                        //BUSY SPIN
                        lastWait = System.nanoTime();
                     }
                     writeHistogram.recordValue(System.nanoTime() - startWrite);
                     waitHistogram.recordValue(lastWait - startWrite);
                  }
                  elapsedRun = System.nanoTime() - startRun;
                  barrier.decrementAndGet();
                  while (barrier.get() != 0) {
                     LockSupport.parkNanos(1L);
                  }

                  synchronized (outputLock) {
                     System.out.println("**********\tHISTOGRAMS RUN " + (r + 1) + " OF [" + Thread.currentThread().getId() + "]\t**********");
                     System.out.println((((long) iterations * pageBytes * 1000_000_000L) / elapsedRun) + " bytes/sec");
                     System.out.println("**********\tWRITE (us)\t**********");
                     writeHistogram.outputPercentileDistribution(System.out, 1000.0);
                     System.out.println("**********\tWAIT (us)\t**********");
                     waitHistogram.outputPercentileDistribution(System.out, 1000.0);
                     System.out.println("**********\tEND HISTOGRAMS RUN " + (r + 1) + " OF [" + Thread.currentThread().getId() + "]\t**********");
                  }
               }
            }
         });
         writerThreads[w] = writerThread;
         writerThread.start();
      }
      for (int i = 0; i < writers; i++) {
         try {
            writerThreads[i].join();
         }
         catch (InterruptedException e) {
            System.err.println(e);
         }
      }
      try {
         appenderThread.join();
      }
      catch (InterruptedException e) {
         System.err.println(e);
      }
   }
}
