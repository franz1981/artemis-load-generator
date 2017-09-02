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
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;

final class ProducerRunner {

   private ProducerRunner() {
   }

   public static void runJmsProducer(Session session,
                                     TimeProvider timeProvider,
                                     int messageBytes,
                                     Destination destination,
                                     int targetThoughput,
                                     int iterations,
                                     int runs,
                                     int warmupIterations,
                                     int waitSecondsBetweenIterations,
                                     boolean isWaitRate,
                                     Delivery delivery,
                                     final AtomicLong sentMessages) {
      MessageProducer producer = null;
      try (final CloseableTickerEventListener tickerEventListener = CloseableTickerEventListener.blackHole()) {
         producer = session.createProducer(destination);
         producer.setDisableMessageTimestamp(true);
         switch (delivery) {
            case Persistent:
               producer.setDeliveryMode(DeliveryMode.PERSISTENT);
               break;
            case NonPersistent:
               producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
               break;
            default:
               throw new AssertionError("unsupported case!");
         }
         final BytesMessage message = session.createBytesMessage();
         final ByteBuffer clientContent = ByteBuffer.allocate(messageBytes).order(ByteOrder.nativeOrder());
         final Ticker ticker;
         final Ticker.ServiceAction serviceAction;
         final MessageProducer localProducer = producer;
         switch (timeProvider) {
            case Nano:
               serviceAction = (intendedStartTime, startServiceTime) -> {
                  try {
                     message.clearBody();
                     BytesMessageUtil.encodeTimestamp(message, clientContent, startServiceTime);
                     localProducer.send(message);
                     sentMessages.lazySet(sentMessages.get() + 1L);
                  } catch (Throwable ex) {
                     System.err.println(ex);
                  }
               };
               break;
            case Millis:
               serviceAction = (intendedStartTime, startServiceTime) -> {
                  try {
                     final long startTime = System.currentTimeMillis();
                     message.clearBody();
                     BytesMessageUtil.encodeTimestamp(message, clientContent, startTime);
                     localProducer.send(message);
                     sentMessages.lazySet(sentMessages.get() + 1L);
                  } catch (Throwable ex) {
                     System.err.println(ex);
                  }
               };
               break;
            default:
               throw new AssertionError("unsupported case!");
         }
         if (targetThoughput > 0) {
            ticker = Ticker.responseUnderLoadBenchmark(serviceAction, tickerEventListener, targetThoughput, iterations, runs, warmupIterations, waitSecondsBetweenIterations, isWaitRate);
         } else {
            ticker = Ticker.throughputBenchmark(serviceAction, tickerEventListener, iterations, runs, warmupIterations, waitSecondsBetweenIterations, isWaitRate);
         }
         ticker.run();
      } catch (JMSException e) {
         throw new IllegalStateException(e);
      } finally {
         CloseableHelper.quietClose(producer);
      }
   }

}
