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

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

import org.HdrHistogram.Histogram;

final class ConsumerLatencyRecorderTask implements Runnable {

   private final CyclicBarrier onStart;
   private final CyclicBarrier onFinished;
   private final DestinationBench.BenchmarkConfiguration conf;
   private final Destination destination;
   private final AtomicLong receivedMessages;
   private final Session session;
   private final Histogram[] latencyHistograms;
   private final CyclicBarrier onMessagesConsumed;
   private final AtomicLong messagesConsumed;
   private final long expectedMessagesPerWarmup;
   private final long expectedMessagesPerRun;
   private static final long RECEIVE_TIMEOUT_MILLIS = 100L;

   ConsumerLatencyRecorderTask(final CyclicBarrier onStart,
                               final CyclicBarrier onFinished,
                               final CyclicBarrier onMessagesConsumed,
                               final AtomicLong messagesConsumedOnDestination,
                               final long expectedMessagesPerWarmup,
                               final long expectedMessagesPerRun,
                               final DestinationBench.BenchmarkConfiguration conf,
                               final Session session,
                               final Destination destination,
                               final AtomicLong receivedMessages,
                               final Histogram[] latencyHistograms) {
      this.onStart = onStart;
      this.onFinished = onFinished;
      this.onMessagesConsumed = onMessagesConsumed;
      this.messagesConsumed = messagesConsumedOnDestination;
      this.expectedMessagesPerRun = expectedMessagesPerRun;
      this.expectedMessagesPerWarmup = expectedMessagesPerWarmup;
      this.conf = conf;
      this.session = session;
      this.destination = destination;
      this.receivedMessages = receivedMessages;
      this.latencyHistograms = latencyHistograms;
      if (this.latencyHistograms.length != (conf.runs + 1)) {
         throw new IllegalArgumentException("latencyHistograms must be " + (conf.runs + 1) + "!");
      }
   }

   @Override
   public void run() {
      final ByteBuffer contentBuffer = ByteBuffer.allocate(conf.messageBytes).order(ByteOrder.nativeOrder());
      MessageConsumer consumer = null;
      try {
         consumer = createConsumer();
         final int runs = conf.runs;
         long receivedMessages = 0;
         for (Histogram histogram : latencyHistograms) {
            histogram.reset();
         }
         final Histogram warmupHistogram = latencyHistograms[0];
         onStart.await();
         while (messagesConsumed.get() != expectedMessagesPerWarmup) {
            final Message received = consumer.receive(RECEIVE_TIMEOUT_MILLIS);
            if (received != null) {
               onReceivedMessage(received, contentBuffer, warmupHistogram);
               receivedMessages++;
               this.receivedMessages.lazySet(receivedMessages);
               if (messagesConsumed.incrementAndGet() == expectedMessagesPerWarmup) {
                  //wait until all the other destination will finish
                  onMessagesConsumed.await();
               }
            }
         }
         onFinished.await();
         for (int r = 0; r < runs; r++) {
            final Histogram runHistogram = latencyHistograms[r + 1];
            onStart.await();
            while (messagesConsumed.get() != expectedMessagesPerRun) {
               final Message received = consumer.receive(RECEIVE_TIMEOUT_MILLIS);
               if (received != null) {
                  onReceivedMessage(received, contentBuffer, runHistogram);
                  receivedMessages++;
                  this.receivedMessages.lazySet(receivedMessages);
                  if (messagesConsumed.incrementAndGet() == expectedMessagesPerRun) {
                     //wait until all the other destination will finish
                     onMessagesConsumed.await();
                  }
               }
            }
            onFinished.await();
         }
      } catch (BrokenBarrierException | InterruptedException | JMSException e)

      {
         e.printStackTrace();
         throw new IllegalStateException(e);
      } finally

      {
         CloseableHelper.quietClose(consumer);
      }

   }

   private static void onReceivedMessage(Message message, ByteBuffer contentBuffer, Histogram histogram) {
      final long receivedTime = System.nanoTime();
      final long startTime = BytesMessageUtil.decodeTimestamp((BytesMessage) message, contentBuffer);
      final long highestTrackableValue = histogram.getHighestTrackableValue();
      final long elapsedTime = receivedTime - startTime;
      final long normalizedElapsedTime = Math.min(highestTrackableValue, elapsedTime);
      histogram.recordValue(normalizedElapsedTime);
   }

   private MessageConsumer createConsumer() throws JMSException {
      if (conf.durableName != null) {
         return session.createDurableSubscriber((Topic) destination, conf.durableName);
      } else {
         return session.createConsumer(destination);
      }
   }
}
