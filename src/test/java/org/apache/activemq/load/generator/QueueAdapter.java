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

import java.util.ArrayDeque;
import java.util.LinkedList;
import java.util.function.Consumer;

import org.apache.activemq.artemis.utils.collections.LinkedListImpl;
import org.jctools.queues.SpscUnboundedArrayQueue;

public interface QueueAdapter<E> {

   void offer(E e);

   void offerFirst(E e);

   E poll();

   void clear();

   default long drain(Consumer<? super E> onMessage) {
      final long size = size();
      for (long i = 0; i < size; i++) {
         onMessage.accept(poll());
      }
      return size;
   }

   long size();

   static <T> QueueAdapter<T> jcLinkedList(LinkedList<T> list) {
      return new QueueAdapter<T>() {
         @Override
         public void offer(T t) {
            list.offer(t);
         }

         @Override
         public void offerFirst(T t) {
            list.addFirst(t);
         }

         @Override
         public T poll() {
            return list.poll();
         }

         @Override
         public long size() {
            return list.size();
         }

         @Override
         public void clear() {
            list.clear();
         }
      };
   }

   static <T> QueueAdapter<T> artemisLinkedList(LinkedListImpl<T> list) {
      return new QueueAdapter<T>() {
         @Override
         public void offer(T t) {
            list.addTail(t);
         }

         @Override
         public void offerFirst(T t) {
            list.addHead(t);
         }

         @Override
         public T poll() {
            return list.poll();
         }

         @Override
         public long size() {
            return list.size();
         }

         @Override
         public void clear() {
            list.clear();
         }
      };
   }

   static <T> QueueAdapter<T> jcArrayDeque(ArrayDeque<T> q) {
      return new QueueAdapter<T>() {
         @Override
         public void offer(T t) {
            q.offer(t);
         }

         @Override
         public void offerFirst(T t) {
            q.offerFirst(t);
         }

         @Override
         public T poll() {
            return q.poll();
         }

         @Override
         public long size() {
            return q.size();
         }

         @Override
         public void clear() {
            q.clear();
         }
      };
   }

   static <T> QueueAdapter<T> chunkedQueue(ChunkedQueue<T> q) {
      return new QueueAdapter<T>() {
         @Override
         public void offer(T t) {
            q.offer(t);
         }

         @Override
         public void offerFirst(T t) {
            q.offerFirst(t);
         }

         @Override
         public T poll() {
            return q.poll();
         }

         @Override
         public long size() {
            return q.size();
         }

         @Override
         public void clear() {
            q.clear();
         }
      };
   }

   static <T> QueueAdapter<T> spscUnboundedArrayQueue(SpscUnboundedArrayQueue<T> q) {
      return new QueueAdapter<T>() {
         @Override
         public void offer(T t) {
            q.offer(t);
         }

         @Override
         public void offerFirst(T t) {
            q.offer(t);
         }

         @Override
         public T poll() {
            return q.poll();
         }

         @Override
         public long size() {
            return q.size();
         }

         @Override
         public void clear() {
            q.clear();
         }
      };
   }

}
