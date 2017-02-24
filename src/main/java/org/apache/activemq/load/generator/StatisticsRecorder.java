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

final class StatisticsRecorder implements AutoCloseable {

   private static final int MAX_CHUNK_SIZE = 1<<30;
   private static final int TIME_OFFSET = 0;
   private static final int TIME_SIZE = Long.BYTES;
   private static final int VALUE_OFFSET = TIME_OFFSET + TIME_SIZE;
   private static final int VALUE_SIZE = Long.BYTES;
   public static final int ENTRY_SIZE = VALUE_OFFSET + VALUE_SIZE;

   static{
      if((MAX_CHUNK_SIZE%ENTRY_SIZE)!=0){
         throw new RuntimeException("MAX_CHUNK_SIZE must be a multiple of ENTRY_SIZE!");
      }
   }

   private MappedByteBuffer[] buffers;
   private final long size;
   private long offset;
   private int mappedRegionIndex;
   private long nextAddress;
   private long nextExclusiveLimit;

   public StatisticsRecorder(File statisticsFile, long samples, boolean fill) {
      this.size = samples*ENTRY_SIZE;
      try (RandomAccessFile raf = new RandomAccessFile(statisticsFile, "rw"); FileChannel fc = raf.getChannel()) {
         buffers = new MappedByteBuffer[(int)((this.size /MAX_CHUNK_SIZE)+1)];
         int i = 0;
         long remaining = this.size;
         long position = 0;
         while(remaining>=MAX_CHUNK_SIZE){
            final MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_WRITE, position , MAX_CHUNK_SIZE);
            buffer.order(ByteOrder.nativeOrder());
            buffers[i] = buffer;
            remaining-=MAX_CHUNK_SIZE;
            position+=MAX_CHUNK_SIZE;
            i++;
         }
         final MappedByteBuffer lastBuffer = fc.map(FileChannel.MapMode.READ_WRITE, position, remaining);
         buffers[i] = lastBuffer;
      } catch (IOException e) {
         throw new IllegalStateException(e);
      }
      if (fill) {
         for(int i = 0;i<buffers.length;i++){
            final MappedByteBuffer buffer = buffers[i];
            final long address = ((DirectBuffer)buffer).address();
            final int capacity = buffer.capacity();
            UnsafeAccess.UNSAFE.setMemory(null,address,capacity,(byte)0);
         }
      }
      this.offset = 0;
      final MappedByteBuffer firstBuffer = buffers[0];
      this.nextAddress = ((DirectBuffer)firstBuffer).address();
      this.nextExclusiveLimit = nextAddress + firstBuffer.capacity();
   }

   public MappedByteBuffer[] buffers(){
      return buffers;
   }

   public long offset() {
      return offset;
   }

   public long size(){
      return size;
   }

   public void appendSample(long time, long value) {
      if(buffers==null){
         throw new IllegalStateException("recorder closed!");
      }
      final long nextOffset = offset + ENTRY_SIZE;
      if(nextOffset> size){
         throw new IllegalStateException("can't add new samples!");
      }
      UnsafeAccess.UNSAFE.putLong(nextAddress + TIME_OFFSET, time);
      UnsafeAccess.UNSAFE.putLong(nextAddress + VALUE_OFFSET, value);
      this.offset = nextOffset;
      final long address = nextAddress + ENTRY_SIZE;
      if(address>=this.nextExclusiveLimit){
         nextMappedRegion();
      }else{
         this.nextAddress = address;
      }
   }

   private void nextMappedRegion(){
      final int nextMappedregionIndex = this.mappedRegionIndex+1;
      if(nextMappedregionIndex<this.buffers.length){
         final MappedByteBuffer buffer = this.buffers[nextMappedregionIndex];
         this.nextAddress = ((DirectBuffer)buffer).address();
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
               }
               catch (Exception e) {
                  System.err.println(e);
               }
            }
         }finally {
            this.buffers = null;
         }
      }
   }
}
