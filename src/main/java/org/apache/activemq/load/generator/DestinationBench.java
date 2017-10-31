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
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import net.openhft.affinity.AffinityLock;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;

public class DestinationBench {

   private static long total(AtomicLong[] count) {
      long total = 0;
      for (AtomicLong number : count) {
         total += number.get();
      }
      return total;
   }

   private static Thread createReportingThreadOf(AtomicLong[] sentMessages,
                                                 AtomicLong[] receivedMessages,
                                                 boolean refresh) {
      final Thread reportingThread = new Thread(() -> {
         long lastSentMessages = total(sentMessages);
         long lastReceivedMessages = total(receivedMessages);
         long lastTimestamp = System.currentTimeMillis();
         while (!Thread.currentThread().isInterrupted()) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            final long now = System.currentTimeMillis();
            final long elapsed = now - lastTimestamp;
            final long sentNow = total(sentMessages);
            final long receivedNow = total(receivedMessages);
            final long sent = sentNow - lastSentMessages;
            final long received = receivedNow - lastReceivedMessages;
            if (refresh) {
               System.out.print("\033[H\033[2J");
            }
            System.out.format("Duration %dms - Sent %,d msg - Received %,d msg%n", elapsed, sent, received);
            lastSentMessages = sentNow;
            lastReceivedMessages = receivedNow;
            lastTimestamp = now;
         }
      });
      reportingThread.setDaemon(true);
      return reportingThread;
   }

   public static final class BenchmarkConfiguration {

      File outputFile = null;
      boolean isWaitRate = false;
      int messageBytes = 100;
      int targetThoughput = 0;
      int runs = 5;
      int warmupIterations = 20_000;
      int iterations = 20_000;
      //1 queue for each 1 producer/consumer pairs
      int partitions = 1;
      int waitSecondsBetweenIterations = 2;
      String destinationName = null;
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
      int forks = 1;
      long messages = ((iterations * runs) + warmupIterations);
      boolean refresh = false;

      boolean read(String[] args) {
         boolean askedForHelp = false;
         for (int i = 0; i < args.length; ++i) {
            final String arg = args[i];
            switch (arg) {
               case "--share-connection":
                  shareConnection = true;
                  break;
               case "--no-producer":
                  producer = false;
                  break;
               case "--refresh":
                  refresh = true;
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
               case "--partitions":
                  partitions = Integer.parseInt(args[++i]);
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
               case "--forks":
                  forks = Integer.parseInt(args[++i]);
                  break;
               case "--help":
                  askedForHelp = true;
                  break;
               default:
                  throw new AssertionError("Invalid args: " + args[i] + " try --help");
            }
         }
         //force shared connection to be true when temp queues/topics
         if (isTemp) {
            //force a shared connection
            shareConnection = true;
         }
         //refresh messages
         messages = ((iterations * runs) + warmupIterations);
         if (askedForHelp) {
            final String validArgs = "\"[--protocol " + Arrays.toString(Protocol.values()) + "][--topic] [--wait-rate] [--persistent] [--time Nano|Millis] [--sample " + Arrays.toString(SampleMode.values()) + "] [--out outputFile] [--url url] [--name destinationName] [--target targetThroughput] [--runs runs] " + "[--iterations iterations] [--warmup warmupIterations] [--bytes messageBytes] [--wait waitSecondsBetweenIterations] [--forks numberOfForks] [--partitions numberOfDestinations] [--refresh]\"";
            System.err.println("valid arguments = " + validArgs);
            return false;
         }
         return true;
      }

      void printSummary() {
         System.out.println("*********\tCONFIGURATION SUMMARY\t*********");
         System.out.println("protocol = " + protocol);
         System.out.println("delivery = " + delivery);
         System.out.println("consumer sample mode: " + sampleMode);
         if (outputFile != null) {
            System.out.println("consumer statistics file = " + outputFile);
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
      }
   }

   public static void main(String[] args) throws Exception {
      final BenchmarkConfiguration conf = new BenchmarkConfiguration();
      if (!conf.read(args)) {
         return;
      }
      conf.printSummary();
      final AtomicLong[] sentMessages = new AtomicLong[conf.forks];
      final AtomicLong[] receivedMessages = new AtomicLong[conf.forks];
      //initialize perf counters
      for (int i = 0; i < conf.forks; i++) {
         sentMessages[i] = new AtomicLong(0);
         receivedMessages[i] = new AtomicLong(0);
      }
      final Thread reportingThread = createReportingThreadOf(sentMessages, receivedMessages, conf.refresh);
      reportingThread.start();
      final Thread[] producerRunners = new Thread[conf.forks];
      final JmsMessageHistogramLatencyRecorder.BenchmarkResult[] results = new JmsMessageHistogramLatencyRecorder.BenchmarkResult[conf.forks];
      final CountDownLatch consumersReady = conf.consumer ? new CountDownLatch(conf.forks) : null;
      for (int i = 0; i < conf.forks; i++) {
         final int forkIndex = i;
         producerRunners[i] = new Thread(() -> {
            runFork(conf, forkIndex, results, consumersReady, sentMessages[forkIndex], receivedMessages[forkIndex]);
         });
      }
      //start the forks
      Stream.of(producerRunners).forEach(Thread::start);
      //wait the forks to finish
      Stream.of(producerRunners).forEach(t -> {
         try {
            t.join();
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
      });
      reportingThread.interrupt();
      reportingThread.join();
      //collect all the results
      final JmsMessageHistogramLatencyRecorder.BenchmarkResult benchmarkResult = JmsMessageHistogramLatencyRecorder.BenchmarkResult.merge(results, conf.runs + 1);
      final PrintStream log;
      if (conf.outputFile != null) {
         log = new PrintStream(new FileOutputStream(conf.outputFile));
      } else {
         log = System.out;
      }
      benchmarkResult.print(log, conf.latencyFormat);
      if (conf.outputFile != null) {
         log.close();
      }
   }

   private static void runFork(BenchmarkConfiguration conf,
                               int forkIndex,
                               JmsMessageHistogramLatencyRecorder.BenchmarkResult[] results,
                               CountDownLatch consumersReady,
                               AtomicLong sentMessages,
                               AtomicLong receivedMessages) {
      try {
         final String forkedDestinationName;
         if (conf.forks > 1) {
            final int nameIndex = forkIndex % conf.partitions;
            forkedDestinationName = conf.destinationName + "_" + nameIndex;
            System.out.println("Bounded destination [" + forkedDestinationName + "] -> [" + forkIndex + "] fork");
         } else {
            forkedDestinationName = conf.destinationName;
         }
         final Consumer<? super JmsMessageHistogramLatencyRecorder.BenchmarkResult> onResult = result -> results[forkIndex] = result;
         final ConnectionFactory connectionFactory = conf.protocol.createConnectionFactory(conf.url);
         final Connection connection = connectionFactory.createConnection();
         final Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final Destination destination;
         if (!conf.isTemp) {
            if (conf.isTopic) {
               destination = conf.protocol.createTopic(forkedDestinationName);
            } else {
               destination = conf.protocol.createQueue(forkedDestinationName);
            }
         } else {
            if (conf.isTopic) {
               destination = producerSession.createTemporaryTopic();
            } else {
               destination = producerSession.createTemporaryQueue();
            }
         }
         if (conf.shareConnection && conf.durableName != null) {
            connection.setClientID(conf.durableName);
         }
         connection.start();
         try {
            if (conf.consumer) {
               final CountDownLatch consumed = new CountDownLatch(1);
               try (final CloseableMessageListener messageListener = CloseableMessageListeners.with(conf, onResult)) {
                  final Connection consumerConnection;
                  if (!conf.shareConnection) {
                     //do not share the connection/connectionFactory (like in a different process)
                     final ConnectionFactory consumerConnectionFactory = conf.protocol.createConnectionFactory(conf.url);
                     consumerConnection = consumerConnectionFactory.createConnection();
                     if (conf.durableName != null) {
                        consumerConnection.setClientID(conf.durableName);
                     }
                     consumerConnection.start();
                  } else {
                     consumerConnection = connection;
                  }
                  final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                  final Agent jmsConsumerAgent = new JmsConsumerAgent("jms_message_consumer@" + forkedDestinationName, consumerSession, destination, messageListener, Integer.MAX_VALUE, conf.messages, consumed, conf.blockingRead, conf.durableName, receivedMessages);
                  try (final AgentRunner consumerRunner = new AgentRunner(new BusySpinIdleStrategy(), System.err::println, null, jmsConsumerAgent)) {
                     final Thread consumerThread = new Thread(() -> {
                        try (AffinityLock affinityLock = AffinityLock.acquireLock()) {
                           consumersReady.countDown();
                           consumerRunner.run();
                        }
                     });
                     consumerThread.start();
                     //start producers only when all the consumers are ready
                     consumersReady.await();
                     if (conf.producer) {
                        ProducerRunner.runJmsProducer(conf, producerSession, destination, sentMessages);
                     }
                     consumed.await();
                  } finally {
                     CloseableHelper.quietClose(consumerSession);
                     if (!conf.shareConnection) {
                        CloseableHelper.quietClose(consumerConnection);
                     }
                  }
               }
            } else {
               if (conf.producer) {
                  ProducerRunner.runJmsProducer(conf, producerSession, destination, sentMessages);
               }
            }
         } finally {
            CloseableHelper.quietClose(producerSession);
            CloseableHelper.quietClose(connection);
         }
      } catch (Throwable t) {
         t.printStackTrace();
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
