/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.agrona.UnsafeAccess;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

final class StatisticsReader implements AutoCloseable {

   public static final class Sample {

      private long time;
      private long value;

      public Sample() {
      }

      public long time() {
         return time;
      }

      public long value() {
         return value;
      }
   }

   private static final int MAX_CHUNK_SIZE = 1 << 30;
   private static final int TIME_OFFSET = 0;
   private static final int TIME_SIZE = Long.BYTES;
   private static final int VALUE_OFFSET = TIME_OFFSET + TIME_SIZE;
   private static final int VALUE_SIZE = Long.BYTES;
   public static final int ENTRY_SIZE = VALUE_OFFSET + VALUE_SIZE;

   static {
      if ((MAX_CHUNK_SIZE % ENTRY_SIZE) != 0) {
         throw new RuntimeException("MAX_CHUNK_SIZE must be a multiple of ENTRY_SIZE!");
      }
   }

   private MappedByteBuffer[] buffers;
   private final long size;
   private long offset;
   private int mappedRegionIndex;
   private long nextAddress;
   private long nextExclusiveLimit;

   public StatisticsReader(File statisticsFile) {
      try (RandomAccessFile raf = new RandomAccessFile(statisticsFile, "r"); FileChannel fc = raf.getChannel()) {
         this.size = fc.size();
         buffers = new MappedByteBuffer[(int) ((this.size / MAX_CHUNK_SIZE) + 1)];
         int i = 0;
         long remaining = this.size;
         long position = 0;
         while (remaining >= MAX_CHUNK_SIZE) {
            final MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, MAX_CHUNK_SIZE);
            buffer.order(ByteOrder.nativeOrder());
            buffers[i] = buffer;
            remaining -= MAX_CHUNK_SIZE;
            position += MAX_CHUNK_SIZE;
            i++;
         }
         final MappedByteBuffer lastBuffer = fc.map(FileChannel.MapMode.READ_ONLY, position, remaining);
         buffers[i] = lastBuffer;
      } catch (IOException ex) {
         throw new IllegalStateException(ex);
      }
      this.offset = 0;
      final MappedByteBuffer firstBuffer = buffers[0];
      this.nextAddress = ((DirectBuffer) firstBuffer).address();
      this.nextExclusiveLimit = nextAddress + firstBuffer.capacity();
   }

   public MappedByteBuffer[] buffers() {
      return buffers;
   }

   public long offset() {
      return offset;
   }

   public long size() {
      return size;
   }

   public boolean readUsing(Sample sample) {
      if (buffers == null) {
         throw new IllegalStateException("recorder closed!");
      }
      final long nextOffset = offset + ENTRY_SIZE;
      if (nextOffset > size) {
         return false;
      }
      final long time = UnsafeAccess.UNSAFE.getLong(nextAddress + TIME_OFFSET);
      final long value = UnsafeAccess.UNSAFE.getLong(nextAddress + VALUE_OFFSET);
      try {
         sample.time = time;
         sample.value = value;
         return true;
      } finally {
         this.offset = nextOffset;
         final long address = nextAddress + ENTRY_SIZE;
         if (address >= this.nextExclusiveLimit) {
            nextMappedRegion();
         } else {
            this.nextAddress = address;
         }
      }
   }

   private void nextMappedRegion() {
      final int nextMappedregionIndex = this.mappedRegionIndex + 1;
      if (nextMappedregionIndex < this.buffers.length) {
         final MappedByteBuffer buffer = this.buffers[nextMappedregionIndex];
         this.nextAddress = ((DirectBuffer) buffer).address();
         final long capacity = buffer.capacity();
         this.nextExclusiveLimit = this.nextAddress + capacity;
         this.mappedRegionIndex = nextMappedregionIndex;
      }
   }

   @Override
   public void close() {
      if (buffers != null) {
         try {
            for (int i = 0; i < buffers.length; i++) {
               try {
                  final MappedByteBuffer buffer = buffers[i];
                  final Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
                  if (cleaner != null) {
                     cleaner.clean();
                  }
               } catch (Exception e) {
                  System.err.println(e);
               }
            }
         } finally {
            this.buffers = null;
         }
      }
   }
}
