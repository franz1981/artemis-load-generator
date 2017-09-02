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
import javax.jms.Message;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;

final class JmsMessageHistogramLatencyRecorder implements CloseableMessageListener {

   private static final class RunStatistics {

      private final int expectedMessages;
      private long producerStart;
      private long producerEnd;
      private long consumerStart;
      private long consumerEnd;
      private int messages;
      private final Histogram latencyHistogram;
      private final TimeProvider timeProvider;

      public RunStatistics(int expectedMessages, Histogram latencyHistogram, TimeProvider timeProvider) {
         this.expectedMessages = expectedMessages;
         this.latencyHistogram = latencyHistogram;
         this.timeProvider = timeProvider;
         this.messages = 0;
      }

      private boolean onMessage(final long produceTime) {
         if (messages >= expectedMessages) {
            throw new IllegalStateException("this run is full");
         }
         final long consumeTime = timeProvider.now();
         if (messages == 0) {
            latencyHistogram.setStartTimeStamp(System.currentTimeMillis());
            this.producerStart = produceTime;
            this.consumerStart = consumeTime;
         }
         final boolean lastMessage = messages == expectedMessages - 1;
         if (lastMessage) {
            latencyHistogram.setEndTimeStamp(System.currentTimeMillis());
            this.producerEnd = produceTime;
            this.consumerEnd = consumeTime;
         }
         final long elapsedTime = consumeTime - produceTime;
         latencyHistogram.recordValue(elapsedTime);
         messages++;
         return !lastMessage;
      }

      private long calculateProducerThroughput(TimeUnit timeUnit) {
         final long totalCount = latencyHistogram.getTotalCount();
         if (totalCount != expectedMessages) {
            throw new IllegalStateException("not available yet");
         }
         final long elapsedTime = producerEnd - producerStart;
         final long throughput = (totalCount * (timeProvider.timeUnit().convert(1, timeUnit))) / elapsedTime;
         return throughput;
      }

      private long calculateConsumerThroughput(TimeUnit timeUnit) {
         final long totalCount = latencyHistogram.getTotalCount();
         if (totalCount != expectedMessages) {
            throw new IllegalStateException("not available yet");
         }
         final long elapsedTime = consumerEnd - consumerStart;
         final long throughput = (totalCount * (timeProvider.timeUnit().convert(1, timeUnit))) / elapsedTime;
         return throughput;
      }

      private long calculateEndToEndThroughput(TimeUnit timeUnit) {
         final long totalCount = latencyHistogram.getTotalCount();
         if (totalCount != expectedMessages) {
            throw new IllegalStateException("not available yet");
         }
         final long elapsedTime = consumerEnd - producerStart;
         final long throughput = (totalCount * (timeProvider.timeUnit().convert(1, timeUnit))) / elapsedTime;
         return throughput;
      }
   }

   private final PrintStream log;
   private final ByteBuffer contentBuffer;
   private final RunStatistics[] runStatistics;
   private int currentRun;
   private double outputValueUnitScalingRatio;
   private final OutputFormat outputFormat;

   public JmsMessageHistogramLatencyRecorder(PrintStream log,
                                             OutputFormat outputFormat,
                                             TimeProvider timeProvider,
                                             int warmup,
                                             int runs,
                                             int iterations,
                                             ByteBuffer heapContentBuffer) {
      this.outputFormat = outputFormat;
      final double outputValueUnitScalingRatio;
      switch (timeProvider) {
         case Nano:
            outputValueUnitScalingRatio = 1000d;
            break;
         case Millis:
            outputValueUnitScalingRatio = 0.001d;
            break;
         default:
            throw new AssertionError("unsupported case!");
      }
      this.outputValueUnitScalingRatio = outputValueUnitScalingRatio;
      this.log = log;
      this.runStatistics = new RunStatistics[runs + 1];
      final long highestTrackableValue = timeProvider.timeUnit().convert(10, TimeUnit.SECONDS);
      this.runStatistics[0] = new RunStatistics(warmup, new Histogram(highestTrackableValue, 2), timeProvider);
      for (int i = 1; i < (runs + 1); i++) {
         this.runStatistics[i] = new RunStatistics(iterations, new Histogram(highestTrackableValue, 2), timeProvider);
      }
      this.currentRun = 0;
      this.contentBuffer = heapContentBuffer;
      if (!contentBuffer.hasArray()) {
         throw new IllegalArgumentException("content buffer must be on heap and writable!");
      }
   }

   @Override
   public void onMessage(Message message) {
      final long startTime = BytesMessageUtil.decodeTimestamp((BytesMessage) message, contentBuffer);
      //the first message arrived need to mark the current run
      if (!this.runStatistics[currentRun].onMessage(startTime)) {
         currentRun++;
      }
   }

   @Override
   public void close() {
      for (int i = 0; i < runStatistics.length; i++) {
         final RunStatistics statistics = runStatistics[i];
         log.println("**************");
         if (i == 0) {
            log.println("WARMUP");
         } else {
            log.println("RUN " + i);
         }
         log.println("**************");
         log.println("Producer Throughput: " + statistics.calculateProducerThroughput(TimeUnit.SECONDS) + " ops/sec");
         log.println("Consumer Throughput: " + statistics.calculateConsumerThroughput(TimeUnit.SECONDS) + " ops/sec");
         log.println("EndToEnd Throughput: " + statistics.calculateEndToEndThroughput(TimeUnit.SECONDS) + " ops/sec");
         log.println("EndToEnd SERVICE-TIME Latencies distribution in " + TimeUnit.MICROSECONDS);
         outputFormat.output(statistics.latencyHistogram, log, this.outputValueUnitScalingRatio);
      }
      log.close();
   }
}
