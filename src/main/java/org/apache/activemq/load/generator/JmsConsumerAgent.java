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

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.Topic;
import java.util.concurrent.atomic.AtomicBoolean;

import org.agrona.concurrent.Agent;

final class JmsConsumerAgent implements Agent {

   private final String consumerName;
   private final MessageConsumer consumer;
   private final MessageListener messageListener;
   private final int messageBatchLimit;
   private final long messageCount;
   private final AtomicBoolean onMessageCount;
   private final boolean blockingRead;
   private long sequence;

   public JmsConsumerAgent(final String consumerName,
                           final Session session,
                           final Destination destination,
                           final MessageListener messageListener,
                           final int messageBatchLimit,
                           final long messageCount,
                           final AtomicBoolean onMessageCount,
                           final boolean blockingRead,
                           final String durableName) {
      MessageConsumer consumer = null;
      try {
         if (durableName != null) {
            consumer = session.createDurableSubscriber((Topic) destination, durableName);
         } else {
            consumer = session.createConsumer(destination);
         }
      } catch (JMSException e) {
         CloseableHelper.quietClose(consumer);
         throw new IllegalStateException(e);
      }
      this.consumer = consumer;
      this.messageBatchLimit = messageBatchLimit;
      this.messageCount = messageCount;
      this.messageListener = messageListener;
      this.consumerName = consumerName;
      this.sequence = 0;
      this.onMessageCount = onMessageCount;
      this.blockingRead = blockingRead;
   }

   @Override
   public int doWork() throws Exception {
      if (sequence >= this.messageCount) {
         return 0;
      }
      final boolean blockingRead = this.blockingRead;
      for (int i = 0; i < messageBatchLimit; i++) {
         final Message message;
         if (blockingRead) {
            message = consumer.receive();
         } else {
            message = consumer.receiveNoWait();
         }
         if (message == null) {
            return i;
         } else {
            messageListener.onMessage(message);
            sequence++;
            if (sequence == this.messageCount) {
               onMessageCount.lazySet(true);
               return i;
            }
         }
      }
      return messageBatchLimit;
   }

   @Override
   public void onClose() {
      CloseableHelper.quietClose(consumer);
   }

   @Override
   public String roleName() {
      return consumerName;
   }
}
