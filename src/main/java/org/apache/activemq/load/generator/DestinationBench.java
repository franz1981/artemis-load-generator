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
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.stream.Stream;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.SingleWriterRecorder;

public class DestinationBench {

   private static long total(AtomicLong[] count) {
      long total = 0;
      for (AtomicLong number : count) {
         total += number.get();
      }
      return total;
   }

   private static long getCurrentTimeMsecWithDelay(final long nextReportingTime) throws InterruptedException {
      long now = System.currentTimeMillis();
      if (now < nextReportingTime) {
         Thread.sleep(nextReportingTime - now);
      }
      return now;
   }

   private static Thread createSamplingThreadOf(BenchmarkConfiguration config,
                                                SingleWriterRecorder recorder,
                                                AtomicBoolean stopRecording) {
      final Thread samplingThread = new Thread(() -> {
         try (PrintStream log = new PrintStream(new FileOutputStream(config.outputFile), false)) {
            Histogram intervalHistogram = null;
            HistogramLogWriter histogramLogWriter = new HistogramLogWriter(log);
            histogramLogWriter.outputLogFormatVersion();
            final long startTime = System.currentTimeMillis();
            final long uptimeAtInitialStartTime = ManagementFactory.getRuntimeMXBean().getUptime();
            long now = startTime;
            final long jvmStartTime = now - uptimeAtInitialStartTime;
            final long reportingStartTime = jvmStartTime;
            histogramLogWriter.outputStartTime(reportingStartTime);
            histogramLogWriter.outputLegend();
            histogramLogWriter.setBaseTime(reportingStartTime);
            final long samplingIntervalMillis = config.samplingIntervalMillis;
            long nextReportingTime = startTime + samplingIntervalMillis;

            while (!stopRecording.get()) {
               now = getCurrentTimeMsecWithDelay(nextReportingTime);
               if (now >= nextReportingTime) {
                  intervalHistogram = recorder.getIntervalHistogram(intervalHistogram);
                  while (now >= nextReportingTime) {
                     nextReportingTime += samplingIntervalMillis;
                  }
                  histogramLogWriter.outputIntervalHistogram(intervalHistogram);
               }
            }
         } catch (Throwable e) {
            e.printStackTrace();
         }
      });
      samplingThread.setDaemon(true);
      return samplingThread;
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
      int samplingIntervalMillis = -1;
      boolean isWaitRate = false;
      int messageBytes = 100;
      int targetThoughput = 0;
      int runs = 5;
      int warmupIterations = 20_000;
      int iterations = 20_000;
      //1 queue for each 1 producer/consumer pairs
      int destinations = -1;
      int waitSecondsBetweenIterations = 2;
      String destinationName = null;
      OutputFormat latencyFormat = OutputFormat.LONG;
      String url = null;
      Delivery delivery = Delivery.NonPersistent;
      boolean isTopic = false;
      boolean isTemp = false;
      Protocol protocol = Protocol.artemis;
      boolean shareConnection = false;
      String durableName = null;
      int forks = 1;
      long messages = ((iterations * runs) + warmupIterations);
      boolean refresh = false;

      boolean read(String[] args) {
         boolean askedForHelp = false;
         for (int i = 0; i < args.length; ++i) {
            final String arg = args[i];
            switch (arg) {
               case "--interval":
                  samplingIntervalMillis = Integer.parseInt(args[++i]);
                  break;
               case "--share-connection":
                  shareConnection = true;
                  break;
               case "--refresh":
                  refresh = true;
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
               case "--destinations":
                  destinations = Integer.parseInt(args[++i]);
                  break;
               case "--out":
                  outputFile = new File(args[++i]);
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
         if (destinations <= 0) {
            destinations = forks;
         }
         if (samplingIntervalMillis > 0 && destinations != 1 && forks != 1) {
            throw new IllegalArgumentException("interval latency recording can't works with multiple forks");
         }
         if (samplingIntervalMillis > 0 && outputFile == null) {
            throw new IllegalArgumentException("Please specify an output file if latency recording is enabled");
         }
         if (forks % destinations != 0) {
            throw new IllegalArgumentException("please specify a number of forks multiple of destinations");
         }
         if (forks != destinations && isTopic) {
            throw new UnsupportedOperationException("right now topics are not supported yet for asymmetric scale benchmarks");
         }
         //force shared connection to be true when temp queues/topics
         if (isTemp) {
            //force a shared connection
            shareConnection = true;
         }
         //refresh messages
         messages = ((iterations * runs) + warmupIterations);
         if (askedForHelp) {
            final String validArgs = "\"[--protocol " + Arrays.toString(Protocol.values()) + "][--topic] [--wait-rate] [--persistent] [--time Nano|Millis] [--out outputFile] [--url url] [--name destinationName] [--target targetThroughput] [--runs runs] " + "[--iterations iterations] [--warmup warmupIterations] [--bytes messageBytes] [--wait waitSecondsBetweenIterations] [--forks numberOfForks] [--destinations numberOfDestinations] [--refresh]\"";
            System.err.println("valid arguments = " + validArgs);
            return false;
         }
         return true;
      }

      void printSummary() {
         System.out.println("*********\tCONFIGURATION SUMMARY\t*********");
         System.out.println("protocol = " + protocol);
         System.out.println("delivery = " + delivery);
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
         if (samplingIntervalMillis > 0) {
            System.out.println("sampling interval = " + samplingIntervalMillis + " ms");
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
      final AtomicLong[] receivedMessagesPerDestination = new AtomicLong[conf.destinations];
      for (int i = 0; i < receivedMessagesPerDestination.length; i++) {
         receivedMessagesPerDestination[i] = new AtomicLong(0);
      }
      //initialize perf counters
      for (int i = 0; i < conf.forks; i++) {
         sentMessages[i] = new AtomicLong(0);
         receivedMessages[i] = new AtomicLong(0);
      }
      final Thread reportingThread = createReportingThreadOf(sentMessages, receivedMessages, conf.refresh);
      reportingThread.start();
      final Thread[] producerRunners = new Thread[conf.forks];
      final Histogram[][] results;
      final SingleWriterRecorder latencyRecorder;
      final long highestLatencyAccepted = TimeUnit.HOURS.toNanos(1);
      final AtomicBoolean finishSampling;
      final Thread samplingThreadOf;
      if (conf.samplingIntervalMillis <= 0) {
         samplingThreadOf = null;
         latencyRecorder = null;
         finishSampling = null;
         results = new Histogram[conf.runs + 1][conf.forks];
         long estimatedFootprintInBytes = 0;
         for (int f = 0; f < conf.forks; f++) {
            for (int r = 0; r < conf.runs + 1; r++) {
               final Histogram histogram = new Histogram(highestLatencyAccepted, 2);
               estimatedFootprintInBytes += histogram.getEstimatedFootprintInBytes();
               results[r][f] = histogram;
            }
         }
         System.out.println("Instrumentation of latencies tooks " + estimatedFootprintInBytes + " bytes");
      } else {
         results = null;
         finishSampling = new AtomicBoolean();
         latencyRecorder = new SingleWriterRecorder(highestLatencyAccepted, 2);
         samplingThreadOf = createSamplingThreadOf(conf, latencyRecorder, finishSampling);
         samplingThreadOf.start();
      }
      try {
         //to coordinate the producers on each run to wait until a run is completed: it can work only if there is a consumer able to consume it and that knows the bench config
         final CyclicBarrier runStarted = new CyclicBarrier((conf.forks * 2) + 1);
         final CyclicBarrier runFinished = new CyclicBarrier((conf.forks * 2) + 1);
         //each consumer that consumes the last expected message await on this: the 1 is the additional controller
         final CyclicBarrier messagesPerRunConsumed = new CyclicBarrier(conf.destinations + 1);
         final ConnectionFactory connectionFactory = conf.protocol.createConnectionFactory(conf.url);
         //create the not temporary destinations here avoiding concurrent tries to create them :)
         final Destination[] destinations = createNotTempDestination(conf);
         for (int i = 0; i < conf.forks; i++) {
            final int forkIndex = i;
            final Thread producerRunner = new Thread(() -> {
               runFork(conf, connectionFactory, destinations, forkIndex, results, latencyRecorder, sentMessages[forkIndex], receivedMessages[forkIndex], runStarted, runFinished, messagesPerRunConsumed, receivedMessagesPerDestination);
            });
            producerRunner.setDaemon(true);
            producerRunner.setName("producer_" + forkIndex);
            producerRunners[i] = producerRunner;
         }
         //start the forks
         Stream.of(producerRunners).forEach(Thread::start);
         final long[] end2endThroughput = measureEnd2EndThroughput(conf, runStarted, runFinished, messagesPerRunConsumed, receivedMessagesPerDestination);
         reportingThread.interrupt();
         reportingThread.join();
         if (results != null) {
            final Histogram[] systemHistograms = mergeHistograms(results);
            final PrintStream log;
            if (conf.outputFile != null) {
               log = new PrintStream(new FileOutputStream(conf.outputFile));
            } else {
               log = System.out;
            }
            printResults(log, conf.latencyFormat, systemHistograms, end2endThroughput);
            if (conf.outputFile != null) {
               log.close();
            }
         } else {
            printResults(System.out, conf.latencyFormat, null, end2endThroughput);
         }
         //wait the forks to finish
         Stream.of(producerRunners).forEach(t -> {
            try {
               t.join();
            } catch (InterruptedException e) {
               e.printStackTrace();
            }
         });
      } finally {
         if (conf.samplingIntervalMillis > 0) {
            finishSampling.set(true);
            samplingThreadOf.join();
         }
      }
   }

   private static Destination[] createNotTempDestination(BenchmarkConfiguration conf) {
      if (!conf.isTemp) {
         final Destination[] destinations = new Destination[conf.destinations];
         for (int destinationIndex = 0; destinationIndex < conf.destinations; destinationIndex++) {
            final String forkedDestinationName;
            if (conf.forks > 1) {
               //parition the forks in order to distribute the destinations
               forkedDestinationName = conf.destinationName + "_" + destinationIndex;
            } else {
               forkedDestinationName = conf.destinationName;
            }
            if (conf.isTopic) {
               destinations[destinationIndex] = conf.protocol.createTopic(forkedDestinationName);
            } else {
               destinations[destinationIndex] = conf.protocol.createQueue(forkedDestinationName);
            }
         }
         if (destinations.length == 1) {
            System.out.println("created " + (conf.isTopic ? "topic " : "queue ") + destinations[0]);
         } else {
            System.out.println("created " + (conf.isTopic ? "topic " : "queue ") + destinations[0] + " -> " + destinations[destinations.length - 1]);
         }
         return destinations;
      } else {
         return null;
      }
   }

   private static long[] measureEnd2EndThroughput(BenchmarkConfiguration conf,
                                                  CyclicBarrier runStarted,
                                                  CyclicBarrier runFinished,
                                                  CyclicBarrier messagesPerRunConsumed,
                                                  AtomicLong[] receivedMessagesPerDestination) throws BrokenBarrierException, InterruptedException {
      final long[] end2endThroughput = new long[conf.runs + 1];
      final long totalWarmupMessages = conf.warmupIterations * conf.forks;
      final long totalRunMessages = conf.iterations * conf.forks;
      //measure system throughput
      //warmup
      //it is safe to be do ONLY here (or right after runFinished.await();
      for (AtomicLong receivedMessagePerDestination : receivedMessagesPerDestination) {
         receivedMessagePerDestination.set(0);
      }
      runStarted.await();
      final long startWarmup = System.currentTimeMillis();
      //it will pass when all the destination will consume their messages
      messagesPerRunConsumed.await();
      final long elapsedTimeWarmup = System.currentTimeMillis() - startWarmup;
      end2endThroughput[0] = (1_000L * totalWarmupMessages) / elapsedTimeWarmup;
      runFinished.await();
      //runs
      for (int r = 0; r < conf.runs; r++) {
         //it is safe to be do ONLY here (or right after runFinished.await();
         for (AtomicLong receivedMessagePerDestination : receivedMessagesPerDestination) {
            receivedMessagePerDestination.set(0);
         }
         runStarted.await();
         final long start = System.currentTimeMillis();
         //it will pass when all the destination will consume their messages
         messagesPerRunConsumed.await();
         final long elapsedTime = System.currentTimeMillis() - start;
         end2endThroughput[r + 1] = (1_000L * totalRunMessages) / elapsedTime;
         runFinished.await();
      }
      return end2endThroughput;
   }

   private static void runFork(BenchmarkConfiguration conf,
                               ConnectionFactory connectionFactory,
                               Destination[] destinations,
                               int forkIndex,
                               Histogram[][] results,
                               SingleWriterRecorder latencyRecorder,
                               AtomicLong sentMessages,
                               AtomicLong receivedMessages,
                               CyclicBarrier runStarted,
                               CyclicBarrier runFinished,
                               CyclicBarrier finishedMessagesOnDestination,
                               AtomicLong[] messagesPerDestinations) {
      try {
         //this can work only because conf.forks % conf.destinations == 0 and with queues!
         final long expectedMessagesPerDestinationOnWarmup = (conf.forks / conf.destinations) * conf.warmupIterations;
         final long expectedMessagesPerDestinationOnRun = (conf.forks / conf.destinations) * conf.iterations;
         final int destinationIndex = forkIndex % conf.destinations;
         final AtomicLong messagesPerDestination = messagesPerDestinations[destinationIndex];
         final Connection producerConnection = connectionFactory.createConnection();
         final Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final Destination destination;
         if (!conf.isTemp) {
            destination = destinations[destinationIndex];
         } else {
            if (conf.isTopic) {
               destination = producerSession.createTemporaryTopic();
            } else {
               destination = producerSession.createTemporaryQueue();
            }
         }
         if (conf.shareConnection && conf.durableName != null) {
            producerConnection.setClientID(conf.durableName);
         }
         producerConnection.start();

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
            consumerConnection = producerConnection;
         }
         final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         //that could be avoided!!
         final Histogram[] latencyHistograms;
         if (results != null) {
            latencyHistograms = new Histogram[conf.runs + 1];
            for (int r = 0; r < conf.runs + 1; r++) {
               latencyHistograms[r] = results[r][forkIndex];
            }
         } else {
            latencyHistograms = null;
         }
         final Thread consumerRunner = new Thread(new ConsumerLatencyRecorderTask(runStarted, runFinished, finishedMessagesOnDestination, messagesPerDestination, expectedMessagesPerDestinationOnWarmup, expectedMessagesPerDestinationOnRun, conf, consumerSession, consumerConnection, destination, receivedMessages, latencyHistograms, latencyRecorder));
         consumerRunner.setName("consumer_" + forkIndex);
         consumerRunner.setDaemon(true);
         consumerRunner.start();
         try {
            ProducerRunner.runJmsProducer(conf, producerSession, destination, sentMessages, eventListener(runStarted, runFinished));
         } finally {
            CloseableHelper.quietClose(producerSession);
            if (!conf.shareConnection) {
               CloseableHelper.quietClose(producerConnection);
            }
         }
         consumerRunner.join();
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

   //Histogram[RUN][FORK]
   private static Histogram[] mergeHistograms(Histogram[][] histograms) {
      final Histogram[] latencyHistograms = new Histogram[histograms.length];
      for (int r = 0; r < histograms.length; r++) {
         final Histogram latencyHistogram = new Histogram(2);
         final Histogram[] runHistograms = histograms[r];
         for (int f = 0; f < runHistograms.length; f++) {
            latencyHistogram.add(runHistograms[f]);
         }
         latencyHistograms[r] = latencyHistogram;
      }
      return latencyHistograms;
   }

   private static CloseableTickerEventListener eventListener(CyclicBarrier runStarted, CyclicBarrier runFinished) {
      return new CloseableTickerEventListener() {
         @Override
         public void onServiceTimeSample(int run, long time, long serviceTime) {

         }

         @Override
         public void onFinishedRun(int run) {
            try {
               runFinished.await();
            } catch (BrokenBarrierException | InterruptedException e) {
               e.printStackTrace();
            }
         }

         @Override
         public void onStartedRun(int run) {
            try {
               runStarted.await();
            } catch (BrokenBarrierException | InterruptedException e) {
               e.printStackTrace();
            }
         }
      };
   }

   private static void printResults(PrintStream log,
                                    OutputFormat outputFormat,
                                    Histogram[] latencyHistograms,
                                    long[] endToEndThroughput) {

      for (int i = 0; i < endToEndThroughput.length; i++) {
         log.println("**************");
         if (i == 0) {
            log.print("WARMUP\t");
         } else {
            log.print("RUN " + i + '\t');
         }
         log.println("EndToEnd Throughput: " + endToEndThroughput[i] + " ops/sec");
         log.println("**************");
         if (latencyHistograms != null) {
            log.println("EndToEnd SERVICE-TIME Latencies distribution in " + TimeUnit.MICROSECONDS);
            //ns->us
            outputFormat.output(latencyHistograms[i], log, 1000d);
         }
      }
   }
}
