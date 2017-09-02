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

import java.util.function.Consumer;

/**
 * This is a single-threaded unbounded queue that can grow in fixed size chunks when overloaded and shrink when not needed.<br>
 * It represents the fusion of a {@link java.util.LinkedList} and a {@link java.util.ArrayDeque} providing to be zero garbage
 * (as the latter) when the load is less or equal to the configured {@code chunkSize} and growable/shrinkable otherwise (as the former).<br>
 * When overloaded it will be more cache and GC friendly than a {@link java.util.LinkedList} due to the chunked allocation (less fragmentation and
 * pointer chasing) and the GC nepotism fix while shrinking.
 *
 * @param <E> the element type
 */
public final class ChunkedQueue<E> {

   private E[] producerChunk;
   private long producerIndex;
   private E[] consumerChunk;
   private long consumerIndex;

   private final int chunkSize;

   private static long nextPow2Of(long capacity, long min) {
      if (Long.bitCount(min) != 1) {
         throw new IllegalArgumentException("min must be power of 2!");
      }
      if ((capacity > min) && (Long.bitCount(capacity) == 1)) {
         return capacity;
      }
      long i = min;
      while (i < capacity) {
         i *= 2;
         if (i <= 0)
            //the max allowed long pow of 2
            return 1L << 62;
      }
      return i;
   }

   private ChunkedQueue(int chunkSize) {
      this.chunkSize = (int) nextPow2Of(chunkSize, 8);
      if (this.chunkSize < 0) {
         throw new IllegalArgumentException("chunkSize must be <=" + (1 << 30));
      }
      //the last slot in each chunk is used to point to the next chunk
      this.producerChunk = (E[]) new Object[this.chunkSize + 1];
      this.consumerChunk = this.producerChunk;
      this.producerIndex = 0;
      this.consumerIndex = 0;
   }

   /**
    * The max number of elements of each chunk.
    *
    * @return the size of each chunk.
    */
   public int chunkSize() {
      return chunkSize;
   }

   private void offerSlowPath(E[] currentChunk, int chunkSize, long producerIndex, int nextPosition, E e) {
      //allocate a new chunk
      final E[] newChunk = (E[]) new Object[chunkSize + 1];
      //link it from the current one
      currentChunk[chunkSize] = (E) newChunk;
      //append to the new chunk
      offerTo(newChunk, e, producerIndex, nextPosition);
      //update the chunk
      this.producerChunk = newChunk;
   }

   private void offerFirstSlowPath(E[] currentChunk, int chunkSize, long consumerIndex, int nextPosition, E e) {
      //allocate a new chunk
      final E[] newChunk = (E[]) new Object[chunkSize + 1];
      //link it to the current one
      newChunk[chunkSize] = (E) currentChunk;
      //move it backward
      offerFirstTo(newChunk, e, consumerIndex, nextPosition);
      this.consumerChunk = newChunk;
   }

   private static int indexOf(long index, long mask) {
      //fast mod -> it doesn't involve any div CPU unit
      return (int) (index & mask);
   }

   /**
    * Inserts the specified element at the start of this queue.
    *
    * @param e the element to append
    */
   public void offerFirst(E e) {
      if (e == null) {
         throw new NullPointerException("e can't be null!");
      }
      final E[] consumerChunk = this.consumerChunk;
      final int chunkSize = this.chunkSize;
      final long consumerIndex = this.consumerIndex;
      //check if the previous position is available
      final int position = indexOf(consumerIndex - 1, chunkSize - 1);
      if (consumerChunk[position] != null) {
         //create a new chunk that point to this one
         offerFirstSlowPath(consumerChunk, chunkSize, consumerIndex, position, e);
      } else {
         offerFirstTo(consumerChunk, e, consumerIndex, position);
      }
   }

   private void offerFirstTo(E[] buffer, E e, long index, int offset) {
      buffer[offset] = e;
      this.consumerIndex = index - 1;
   }

   /**
    * Inserts the specified element at the end of this queue.
    *
    * @param e the element to append
    */
   public void offer(E e) {
      if (e == null) {
         throw new NullPointerException("e can't be null!");
      }
      final E[] producerChunk = this.producerChunk;
      final int chunkSize = this.chunkSize;
      final long producerIndex = this.producerIndex;
      final int nextPosition = indexOf(producerIndex, chunkSize - 1);
      //if there is already an element in the next slot is needed to append a new chunk
      if (producerChunk[nextPosition] != null) {
         offerSlowPath(producerChunk, chunkSize, producerIndex, nextPosition, e);
      } else {
         //let the append to happen in the same position in the new chunk too
         offerTo(producerChunk, e, producerIndex, nextPosition);
      }
   }

   private void offerTo(E[] buffer, E e, long index, int offset) {
      buffer[offset] = e;
      this.producerIndex = index + 1;
   }

   /**
    * Clear each element of the queue.
    */
   public void clear() {
      //must walk through the queue in order to avoid GC nepotism: could be used Arrays.fill too to bulk clean each chunk!
      drain(m -> {
      });
   }

   /**
    * Retrieves and removes the head (ie the first element) of the queue, or returns
    * {@code null} if this queue is empty.<br>
    *
    * @return the head of the queue represented by this queue, or
    * {@code null} if this queue is empty
    */
   public E poll() {
      final int nextPosition = indexOf(this.consumerIndex, this.chunkSize - 1);
      final E e = this.consumerChunk[nextPosition];
      if (e != null) {
         //clean it to help the producer to find available space into it
         this.consumerChunk[nextPosition] = null;
         this.consumerIndex++;
         return e;
      } else {
         //it is really empty or is necessary to move on the next chunk
         return pollOnNextChunk(nextPosition);
      }
   }

   private E pollOnNextChunk(int nextPosition) {
      //no need to free nextPosition, it is already null!
      //check if there is a next chunk
      final E[] nextChunk = (E[]) this.consumerChunk[this.chunkSize];
      //no next chunk, it is really empty
      if (nextChunk == null) {
         return null;
      } else {
         //avoid GC nepotism cleaning the pointer to the next
         this.consumerChunk[this.chunkSize] = null;
         //move to the next chunk
         this.consumerChunk = nextChunk;
         //maintain the same position to start poll
         final E e = this.consumerChunk[nextPosition];
         //a new chunk can't be empty by definition, no need to check is any element is available
         this.consumerChunk[nextPosition] = null;
         this.consumerIndex++;
         return e;
      }
   }

   /**
    * Remove all available elements from the queue and hand to consume. This should be semantically similar to:
    * <code><br/>
    * M m;</br>
    * while((m = poll()) != null){</br>
    * c.accept(m);</br>
    * }</br>
    * </code>
    * At the end of the operation the queue must be empty.
    *
    * @return the number of polled elements
    */
   public long drain(Consumer<E> onMessage) {
      final long size = size();
      for (long i = 0; i < size; i++) {
         onMessage.accept(poll());
      }
      return size;
   }

   /**
    * Returns the number of elements available in the queue
    *
    * @return the number of elements in the queue
    */
   public long size() {
      return producerIndex - consumerIndex;
   }

   /**
    * Create a {@link ChunkedQueue} with a fixed {@code} chunkSize.
    *
    * @param chunkSize the number of elements of each chunk
    * @param <E>       the element type
    * @return
    */
   public static <E> ChunkedQueue<E> with(int chunkSize) {
      return new ChunkedQueue<>(chunkSize);
   }
}

