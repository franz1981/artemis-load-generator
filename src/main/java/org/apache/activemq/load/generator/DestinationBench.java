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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import java.io.File;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

import net.openhft.affinity.AffinityLock;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;

public class DestinationBench {

   public static void main(String[] args) throws Exception {
      final AtomicLong sentMessages = new AtomicLong(0);
      final AtomicLong receivedMessages = new AtomicLong(0);
      final Thread reportingThread = new Thread(() -> {
         long lastSentMessages = sentMessages.get();
         long lastReceivedMessages = receivedMessages.get();
         long lastTimestamp = System.currentTimeMillis();
         while (!Thread.currentThread().isInterrupted()) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            final long now = System.currentTimeMillis();
            final long elapsed = now - lastTimestamp;
            final long sentNow = sentMessages.get();
            final long receivedNow = receivedMessages.get();
            final long sent = sentNow - lastSentMessages;
            final long received = receivedNow - lastReceivedMessages;
            System.out.print("\033[H\033[2J");
            System.out.format("Duration %dms - Sent %,d msg - Received %,d msg%n", elapsed, sent, received);
            lastSentMessages = sentNow;
            lastReceivedMessages = receivedNow;
            lastTimestamp = now;
         }
      });
      reportingThread.setDaemon(true);
      File outputFile = null;
      boolean isWaitRate = false;
      int messageBytes = 100;
      int targetThoughput = 0;
      int runs = 5;
      int warmupIterations = 20_000;
      int iterations = 20_000;
      int waitSecondsBetweenIterations = 2;
      String destinationName = null;
      boolean askedForHelp = false;
      SampleMode sampleMode = SampleMode.Percentile;
      OutputFormat latencyFormat = OutputFormat.LONG;
      String url = null;
      TimeProvider timeProvider = TimeProvider.Nano;
      Delivery delivery = Delivery.NonPersistent;
      boolean isTopic = false;
      boolean isTemp = false;
      Protocol protocol = Protocol.artemis;
      boolean producer = true;
      boolean consumer = true;
      boolean shareConnection = false;
      boolean blockingRead = true;
      String durableName = null;
      for (int i = 0; i < args.length; ++i) {
         final String arg = args[i];
         switch (arg) {
            case "--share-connection":
               shareConnection = true;
               break;
            case "--no-producer":
               producer = false;
               break;
            case "--no-consumer":
               consumer = false;
               break;
            case "--protocol":
               protocol = Protocol.valueOf(args[++i]);
               break;
            case "--wait-rate":
               isWaitRate = true;
               break;
            case "--persistent":
               delivery = Delivery.Persistent;
               break;
            case "--url":
               url = args[++i];
               break;
            case "--out":
               outputFile = new File(args[++i]);
               break;
            case "--sample":
               sampleMode = SampleMode.valueOf(args[++i]);
               break;
            case "--format":
               latencyFormat = OutputFormat.valueOf(args[++i]);
               break;
            case "--topic":
               isTopic = true;
               break;
            case "--temp":
               isTemp = true;
               break;
            case "--bytes":
               messageBytes = Integer.parseInt(args[++i]);
               break;
            case "--wait":
               waitSecondsBetweenIterations = Integer.parseInt(args[++i]);
               break;
            case "--destination":
               destinationName = args[++i];
               break;
            case "--target":
               targetThoughput = Integer.parseInt(args[++i]);
               break;
            case "--non-blocking-read":
               blockingRead = false;
               break;
            case "--runs":
               runs = Integer.parseInt(args[++i]);
               break;
            case "--iterations":
               iterations = Integer.parseInt(args[++i]);
               break;
            case "--warmup":
               warmupIterations = Integer.parseInt(args[++i]);
               break;
            case "--name":
               destinationName = args[++i];
               break;
            case "--time":
               timeProvider = TimeProvider.valueOf(args[++i]);
               break;
            case "--durable":
               durableName = args[++i];
               break;
            case "--help":
               askedForHelp = true;
               break;
            default:
               throw new AssertionError("Invalid args: " + args[i] + " try --help");
         }
      }
      if (askedForHelp) {
         final String validArgs = "\"[--protocol " + Arrays.toString(Protocol.values()) + "][--topic] [--wait-rate] [--persistent] [--time Nano|Millis] [--sample " + Arrays.toString(SampleMode.values()) + "] [--out outputFile] [--url url] [--name destinationName] [--target targetThroughput] [--runs runs] " + "[--iterations iterations] [--warmup warmupIterations] [--bytes messageBytes] [--wait waitSecondsBetweenIterations]\"";
         System.err.println("valid arguments = " + validArgs);
         if (args.length == 1) {
            return;
         }
      }
      //force shared connection to be true when temp queues/topics
      if (isTemp) {
         //force a shared connection
         shareConnection = true;
      }
      final long messages = ((iterations * runs) + warmupIterations);
      final File consumerStatisticsFile = outputFile;
      //SUMMARY OF CONFIGURATION
      System.out.println("*********\tCONFIGURATION SUMMARY\t*********");
      System.out.println("protocol = " + protocol);
      System.out.println("delivery = " + delivery);
      System.out.println("consumer sample mode: " + sampleMode);
      if (consumerStatisticsFile != null) {
         System.out.println("consumer statistics file = " + consumerStatisticsFile);
      }
      System.out.println("url = " + url);
      System.out.println("destinationType = " + (isTopic ? "Topic" : "Queue"));
      if (!isTemp) {
         System.out.println("destinationName = " + destinationName);
      } else {
         System.out.println("temporary");
      }
      System.out.println("messageBytes = " + messageBytes);
      if (targetThoughput > 0) {
         System.out.println("targetThroughput = " + targetThoughput);
      }
      System.out.println("runs = " + runs);
      System.out.println("warmupIterations = " + warmupIterations);
      System.out.println("iterations = " + iterations);
      System.out.println("share connection = " + shareConnection);
      if (consumer) {
         if (!blockingRead) {
            System.out.println("MessageConsumer::receiveNoWait");
         } else {
            System.out.println("MessageConsumer::receive");
         }
      }
      System.out.println("*********\tEND CONFIGURATION SUMMARY\t*********");
      final ConnectionFactory connectionFactory = protocol.createConnectionFactory(url);
      final Connection connection = connectionFactory.createConnection();
      final Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final Destination destination;
      if (!isTemp) {
         if (isTopic) {
            destination = protocol.createTopic(destinationName);
         } else {
            destination = protocol.createQueue(destinationName);
         }
      } else {
         if (isTopic) {
            destination = producerSession.createTemporaryTopic();
         } else {
            destination = producerSession.createTemporaryQueue();
         }
      }
      if (shareConnection && durableName != null) {
         connection.setClientID(durableName);
      }
      connection.start();
      try {
         if (consumer) {
            final CountDownLatch consumerReady = new CountDownLatch(1);
            final AtomicBoolean consumed = new AtomicBoolean(false);

            try (final CloseableMessageListener messageListener = CloseableMessageListeners.with(timeProvider, messageBytes, consumerStatisticsFile, sampleMode, latencyFormat, iterations, runs, warmupIterations)) {
               final Connection consumerConnection;
               if (!shareConnection) {
                  //do not share the connection/connectionFactory (like in a different process)
                  final ConnectionFactory consumerConnectionFactory = protocol.createConnectionFactory(url);
                  consumerConnection = consumerConnectionFactory.createConnection();
                  if (durableName != null) {
                     consumerConnection.setClientID(durableName);
                  }
                  consumerConnection.start();
               } else {
                  consumerConnection = connection;
               }
               final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
               final Agent jmsConsumerAgent = new JmsConsumerAgent("jms_message_consumer", consumerSession, destination, messageListener, Integer.MAX_VALUE, messages, consumed, blockingRead, durableName, receivedMessages);
               try (final AgentRunner consumerRunner = new AgentRunner(new BusySpinIdleStrategy(), System.err::println, null, jmsConsumerAgent)) {
                  final Thread consumerThread = new Thread(() -> {
                     try (AffinityLock affinityLock = AffinityLock.acquireLock()) {
                        consumerReady.countDown();
                        consumerRunner.run();
                     }
                  });
                  consumerThread.start();
                  //start producer when
                  consumerReady.await();
                  reportingThread.start();
                  if (producer) {
                     ProducerRunner.runJmsProducer(producerSession, timeProvider, messageBytes, destination, targetThoughput, iterations, runs, warmupIterations, waitSecondsBetweenIterations, isWaitRate, delivery, sentMessages);
                  }
                  while (!consumed.get()) {
                     LockSupport.parkNanos(1L);
                  }
               } finally {
                  CloseableHelper.quietClose(consumerSession);
                  if (!shareConnection) {
                     CloseableHelper.quietClose(consumerConnection);
                  }
               }
            }
         } else {
            reportingThread.start();
            if (producer) {
               ProducerRunner.runJmsProducer(producerSession, timeProvider, messageBytes, destination, targetThoughput, iterations, runs, warmupIterations, waitSecondsBetweenIterations, isWaitRate, delivery, sentMessages);
            }
         }
      } finally {
         CloseableHelper.quietClose(producerSession);
         CloseableHelper.quietClose(connection);
      }

   }

   enum Protocol {
      artemis(org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory::new, org.apache.activemq.artemis.jms.client.ActiveMQQueue::new, org.apache.activemq.artemis.jms.client.ActiveMQTopic::new), amqp(org.apache.qpid.jms.JmsConnectionFactory::new, org.apache.qpid.jms.JmsQueue::new, org.apache.qpid.jms.JmsTopic::new), open_wire(org.apache.activemq.ActiveMQConnectionFactory::new, org.apache.activemq.command.ActiveMQQueue::new, org.apache.activemq.command.ActiveMQTopic::new);

      private final Function<String, ? extends ConnectionFactory> factory;
      private final Function<String, ? extends Queue> queueFactory;
      private final Function<String, ? extends Topic> topicFactory;

      Protocol(Function<String, ? extends ConnectionFactory> factory,
               Function<String, ? extends Queue> queueFactory,
               Function<String, ? extends Topic> topicFactory) {
         this.factory = factory;
         this.queueFactory = queueFactory;
         this.topicFactory = topicFactory;
      }

      ConnectionFactory createConnectionFactory(String uri) {
         return factory.apply(uri);
      }

      Queue createQueue(String name) {
         return this.queueFactory.apply(name);
      }

      Topic createTopic(String name) {
         return this.topicFactory.apply(name);
      }
   }

}
