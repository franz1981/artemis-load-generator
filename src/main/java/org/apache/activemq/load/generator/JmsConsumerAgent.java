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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import java.util.concurrent.atomic.AtomicBoolean;

import org.agrona.concurrent.Agent;

final class JmsConsumerAgent implements Agent {

   private final String consumerName;
   private final Connection connection;
   private final Session session;
   private final MessageConsumer consumer;
   private final MessageListener messageListener;
   private final int messageBatchLimit;
   private final long messageCount;
   private final AtomicBoolean onMessageCount;
   private long sequence;

   public JmsConsumerAgent(final String consumerName,
                           final ConnectionFactory connectionFactory,
                           final Destination destination,
                           final MessageListener messageListener,
                           final int messageBatchLimit,
                           final long messageCount,
                           final AtomicBoolean onMessageCount) {
      Connection connection = null;
      Session session = null;
      MessageConsumer consumer = null;
      try {
         connection = connectionFactory.createConnection();
         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         consumer = session.createConsumer(destination);
         connection.start();
      }
      catch (JMSException e) {
         CloseableHelper.quietClose(consumer);
         CloseableHelper.quietClose(session);
         CloseableHelper.quietClose(connection);
         throw new IllegalStateException(e);
      }
      this.connection = connection;
      this.session = session;
      this.consumer = consumer;
      this.messageBatchLimit = messageBatchLimit;
      this.messageCount = messageCount;
      this.messageListener = messageListener;
      this.consumerName = consumerName;
      this.sequence = 0;
      this.onMessageCount = onMessageCount;
   }

   @Override
   public int doWork() throws Exception {
      if (sequence >= this.messageCount) {
         return 0;
      }
      for (int i = 0; i < messageBatchLimit; i++) {
         final Message message = consumer.receiveNoWait();
         if (message == null) {
            return i;
         }
         else {
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
      CloseableHelper.quietClose(session);
      CloseableHelper.quietClose(connection);
   }

   @Override
   public String roleName() {
      return consumerName;
   }
}
