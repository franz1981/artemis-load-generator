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
import javax.jms.Connection;
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
import org.HdrHistogram.SingleWriterRecorder;

final class ConsumerLatencyRecorderTask implements Runnable {

   private final CyclicBarrier onStart;
   private final CyclicBarrier onFinished;
   private final DestinationBench.BenchmarkConfiguration conf;
   private final Destination destination;
   private final AtomicLong receivedMessages;
   private final Session session;
   private final Connection connection;
   private final Histogram[] latencyHistograms;
   private final CyclicBarrier onMessagesConsumed;
   private final AtomicLong messagesConsumed;
   private final long expectedMessagesPerWarmup;
   private final long expectedMessagesPerRun;
   private final SingleWriterRecorder latencyRecorder;
   private static final long RECEIVE_TIMEOUT_MILLIS = 100L;

   ConsumerLatencyRecorderTask(final CyclicBarrier onStart,
                               final CyclicBarrier onFinished,
                               final CyclicBarrier onMessagesConsumed,
                               final AtomicLong messagesConsumedOnDestination,
                               final long expectedMessagesPerWarmup,
                               final long expectedMessagesPerRun,
                               final DestinationBench.BenchmarkConfiguration conf,
                               final Session session,
                               final Connection connection,
                               final Destination destination,
                               final AtomicLong receivedMessages,
                               final Histogram[] latencyHistograms,
                               final SingleWriterRecorder latencyRecorder) {
      this.onStart = onStart;
      this.onFinished = onFinished;
      this.onMessagesConsumed = onMessagesConsumed;
      this.messagesConsumed = messagesConsumedOnDestination;
      this.expectedMessagesPerRun = expectedMessagesPerRun;
      this.expectedMessagesPerWarmup = expectedMessagesPerWarmup;
      this.conf = conf;
      this.session = session;
      this.connection = connection;
      this.destination = destination;
      this.receivedMessages = receivedMessages;
      this.latencyHistograms = latencyHistograms;
      this.latencyRecorder = latencyRecorder;
      if (this.latencyHistograms != null && this.latencyHistograms.length != (conf.runs + 1)) {
         throw new IllegalArgumentException("latencyHistograms must be " + (conf.runs + 1) + "!");
      }
   }

   @Override
   public void run() {
      final ByteBuffer contentBuffer = ByteBuffer.allocate(conf.messageBytes).order(ByteOrder.nativeOrder());
      MessageConsumer consumer = null;
      final SingleWriterRecorder latencyRecorder = this.latencyRecorder;
      try {
         consumer = createConsumer();
         final int runs = conf.runs;
         long receivedMessages = 0;
         if (latencyHistograms != null) {
            for (Histogram histogram : latencyHistograms) {
               histogram.reset();
            }
         }
         final Histogram warmupHistogram = latencyHistograms != null ? latencyHistograms[0] : null;
         onStart.await();
         while (messagesConsumed.get() != expectedMessagesPerWarmup) {
            final Message received = consumer.receive(RECEIVE_TIMEOUT_MILLIS);
            if (received != null) {
               onReceivedMessage(received, contentBuffer, warmupHistogram, latencyRecorder);
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
            final Histogram runHistogram = latencyHistograms != null ? latencyHistograms[r + 1] : null;
            onStart.await();
            while (messagesConsumed.get() != expectedMessagesPerRun) {
               final Message received = consumer.receive(RECEIVE_TIMEOUT_MILLIS);
               if (received != null) {
                  onReceivedMessage(received, contentBuffer, runHistogram, latencyRecorder);
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
         CloseableHelper.quietClose(session);
         if (!conf.shareConnection) {
            CloseableHelper.quietClose(connection);
         }
      }

   }

   private static void onReceivedMessage(Message message,
                                         ByteBuffer contentBuffer,
                                         Histogram histogram,
                                         SingleWriterRecorder latencyRecorder) {
      final long receivedTime = System.nanoTime();
      final long startTime = BytesMessageUtil.decodeTimestamp((BytesMessage) message, contentBuffer);
      final long elapsedTime = receivedTime - startTime;
      if (histogram != null) {
         histogram.recordValue(elapsedTime);
      }
      if (latencyRecorder != null) {
         latencyRecorder.recordValue(elapsedTime);
      }
   }

   private MessageConsumer createConsumer() throws JMSException {
      if (conf.durableName != null) {
         return session.createDurableSubscriber((Topic) destination, conf.durableName);
      } else {
         return session.createConsumer(destination);
      }
   }
}
