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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import net.openhft.affinity.AffinityLock;
import org.HdrHistogram.Histogram;
import org.agrona.BufferUtil;
import org.agrona.IoUtil;
import org.agrona.UnsafeAccess;

public class FlushScaleLatencyBench {

   //HINT: needs to be a realistic value <-> a power of 2!
   private static final int OS_PAGE_SIZE = Integer.getInteger("os.page.size", 4096);
   private static final ByteBuffer ZERO_BYTES = ByteBuffer.allocateDirect(OS_PAGE_SIZE).order(ByteOrder.nativeOrder());

   private interface Writer extends AutoCloseable {

      void write(ByteBuffer byteBuffer, long position);

      void sync();

      @Override
      void close();
   }

   private static final class MappedWriter implements Writer {

      private final MappedByteBuffer mappedBytes;
      private final long address;

      public MappedWriter(File file, boolean fillLength, long expectedLength) throws IOException {
         try (final RandomAccessFile raf = new RandomAccessFile(file, "rw"); final FileChannel fc = raf.getChannel()) {
            mappedBytes = fc.map(FileChannel.MapMode.READ_WRITE, 0, expectedLength);
            address = BufferUtil.address(mappedBytes);
         }
         if (fillLength) {
            UnsafeAccess.UNSAFE.setMemory(address, expectedLength, (byte) 0);
            mappedBytes.force();
         }
      }

      @Override
      public void write(final ByteBuffer byteBuffer, long position) {
         final long srcAddress = BufferUtil.address(byteBuffer);
         UnsafeAccess.UNSAFE.copyMemory(srcAddress, address + position, byteBuffer.capacity());
      }

      @Override
      public void sync() {
         mappedBytes.force();
      }

      @Override
      public void close() {
         IoUtil.unmap(mappedBytes);
      }
   }

   private static final class NioWriter implements Writer {

      private final RandomAccessFile raf;
      private final FileChannel fc;

      public NioWriter(File file, boolean presetLength, boolean fillLength, long expectedLength) throws IOException {
         final RandomAccessFile raf = new RandomAccessFile(file, "rw");
         final FileChannel fc = raf.getChannel();
         if (presetLength) {
            raf.setLength(expectedLength);
            raf.getFD().sync();
         }
         if (fillLength) {
            long pos;
            for (pos = 0; pos < expectedLength; pos += OS_PAGE_SIZE) {
               ZERO_BYTES.clear();
               fc.write(ZERO_BYTES, pos);
            }
            final int lastZeroLimit = (int) (expectedLength & (OS_PAGE_SIZE - 1));
            if (lastZeroLimit > 0) {
               ZERO_BYTES.clear().limit(lastZeroLimit);
               fc.write(ZERO_BYTES, pos);
            }
            fc.force(true);
         }
         this.raf = raf;
         this.fc = fc;
      }

      @Override
      public void write(ByteBuffer byteBuffer, long position) {
         try {
            fc.write(byteBuffer, position);
         }
         catch (IOException e) {
            throw new IllegalStateException(e);
         }
      }

      @Override
      public void sync() {
         try {
            fc.force(false);
         }
         catch (IOException e) {
            throw new IllegalStateException(e);
         }
      }

      @Override
      public void close() {
         try {
            this.fc.close();
            this.raf.close();
         }
         catch (IOException ex) {
            throw new IllegalStateException(ex);
         }
      }
   }

   public static void main(String[] args) {
      boolean mappedParam = false;
      int writersParam = 1;
      int pageBytesParam = OS_PAGE_SIZE;
      int iterationsParam = 0;
      int flushParam = 0;
      int runsParam = 5;
      boolean askedForHelp = false;
      boolean presetLengthParam = true;
      boolean fillLengthParam = true;
      String outParam = System.getProperty("java.io.tmpdir");

      for (int i = 0; i < args.length; ++i) {
         final String arg = args[i];
         switch (arg) {
            case "--mapped":
               mappedParam = true;
               break;
            case "--writers":
               writersParam = Integer.parseInt(args[++i]);
               break;
            case "--page-bytes":
               pageBytesParam = Integer.parseInt(args[++i]);
               break;
            case "--flush":
               flushParam = Integer.parseInt(args[++i]);
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

      final boolean mapped = mappedParam;
      final int writers = writersParam;
      final int pageBytes = pageBytesParam;
      final int iterations = iterationsParam;
      if (iterations == 0) {
         throw new IllegalArgumentException("iterations must be>0!");
      }
      final boolean noFlush = flushParam == 0;
      final int unflashedWrites = noFlush ? iterationsParam : flushParam;
      final int its = iterations / unflashedWrites;
      if ((iterations % unflashedWrites) != 0) {
         throw new IllegalArgumentException("iterations must be a multiple of flush!");
      }
      final long expectedLength = ((long) iterations * pageBytes);
      if (mapped && expectedLength > (Integer.MAX_VALUE)) {
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
      if (!noFlush) {
         System.out.println("flushPeriod: " + unflashedWrites);
      }
      System.out.println("pageBytes: " + pageBytes + " bytes");
      System.out.println("presetLength: " + presetLength);
      System.out.println("fillLength: " + fillLength);
      System.out.println("temp file expected length: " + expectedLength + " bytes");
      System.out.println("outFolder: " + outFolder);
      System.out.println("histograms could be plotted on http://hdrhistogram.github.io/HdrHistogram/plotFiles.html");
      System.out.println("***********\tEND CONFIGURATION SUMMARY:\t***********");
      for (int w = 0; w < writers; w++) {
         final Thread writerThread = new Thread(() -> {
            try (AffinityLock affinityLock = AffinityLock.acquireLock()) {
               try {
                  final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(pageBytes).order(ByteOrder.nativeOrder());
                  final Histogram writeHistogram = new Histogram(TimeUnit.MINUTES.toNanos(1), 2);
                  final Histogram flushHistogram = new Histogram(TimeUnit.MINUTES.toNanos(1), 2);
                  for (int r = 0; r < runs; r++) {
                     writeHistogram.reset();
                     flushHistogram.reset();
                     final File file = File.createTempFile("temp", ".journal", outFolder);
                     file.deleteOnExit();
                     final long elapsedRun;
                     final Writer configuredWriter;
                     if (!mapped) {
                        configuredWriter = new NioWriter(file, presetLength, fillLength, expectedLength);
                     }
                     else {
                        configuredWriter = new MappedWriter(file, fillLength, expectedLength);
                     }
                     try (final Writer writer = configuredWriter) {
                        barrier.incrementAndGet();
                        while (barrier.get() != writers) {
                           LockSupport.parkNanos(1L);
                        }
                        long position = 0;
                        final long startRun = System.nanoTime();
                        for (int it = 0; it < its; it++) {
                           for (int i = 0; i < unflashedWrites; i++) {
                              byteBuffer.clear();
                              final long startWrite = System.nanoTime();
                              configuredWriter.write(byteBuffer, position);
                              position += pageBytes;
                              writeHistogram.recordValue(System.nanoTime() - startWrite);
                           }
                           if (!noFlush) {
                              final long startFlush = System.nanoTime();
                              writer.sync();
                              flushHistogram.recordValue(System.nanoTime() - startFlush);
                           }
                        }
                        elapsedRun = System.nanoTime() - startRun;
                        barrier.decrementAndGet();
                        while (barrier.get() != 0) {
                           LockSupport.parkNanos(1L);
                        }
                     }
                     file.delete();
                     synchronized (outputLock) {
                        System.out.println("**********\tHISTOGRAMS RUN " + (r + 1) + " OF [" + Thread.currentThread().getId() + "]\t**********");
                        System.out.println((((long) iterations * pageBytes * 1000_000_000L) / elapsedRun) + " bytes/sec");
                        System.out.println("**********\tWRITE (us)\t**********");
                        writeHistogram.outputPercentileDistribution(System.out, 1000.0);
                        if (!noFlush) {
                           System.out.println("**********\tFLUSH (us)\t**********");
                           flushHistogram.outputPercentileDistribution(System.out, 1000.0);
                        }
                        System.out.println("**********\tEND HISTOGRAMS RUN " + (r + 1) + " OF [" + Thread.currentThread().getId() + "]\t**********");
                     }
                  }
               }
               catch (IOException ex) {
                  System.err.println(ex);
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
   }

}
