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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.HdrHistogram.Histogram;

final class JmsMessageHistogramLatencyRecorder implements CloseableMessageListener {

   public static final class BenchmarkResult {

      public final long[] producerThroughput;
      public final long[] consumerThroughput;
      public final long[] endToEndThroughput;
      public final long[] tooHighSamples;
      public final TimeUnit throughputTimeUnit;
      public final Histogram[] latencyHistograms;
      public double outputValueUnitScalingRatio;
      public final TimeUnit latencyTimeUnit;

      private BenchmarkResult(long[] producerThroughput,
                              long[] consumerThroughput,
                              long[] endToEndThroughput,
                              long[] tooHighSamples,
                              TimeUnit throughputTimeUnit,
                              Histogram[] latencyHistograms,
                              double outputValueUnitScalingRatio,
                              TimeUnit latencyTimeUnit) {
         this.producerThroughput = producerThroughput;
         this.consumerThroughput = consumerThroughput;
         this.endToEndThroughput = endToEndThroughput;
         this.tooHighSamples = tooHighSamples;
         this.throughputTimeUnit = throughputTimeUnit;
         this.latencyHistograms = latencyHistograms;
         this.outputValueUnitScalingRatio = outputValueUnitScalingRatio;
         this.latencyTimeUnit = latencyTimeUnit;
      }

      private static BenchmarkResult extract(RunStatistics[] runStatistics, double outputValueUnitScalingRatio) {
         final long[] producerThroughput = new long[runStatistics.length];
         final long[] consumerThroughput = new long[runStatistics.length];
         final long[] endToEndThroughput = new long[runStatistics.length];
         final long[] tooHighSamples = new long[runStatistics.length];
         final Histogram[] latencyHistograms = new Histogram[runStatistics.length];
         for (int i = 0; i < runStatistics.length; i++) {
            final RunStatistics statistics = runStatistics[i];
            producerThroughput[i] = statistics.calculateProducerThroughput(TimeUnit.SECONDS);
            consumerThroughput[i] = statistics.calculateConsumerThroughput(TimeUnit.SECONDS);
            endToEndThroughput[i] = statistics.calculateEndToEndThroughput(TimeUnit.SECONDS);
            tooHighSamples[i] = statistics.tooHighSamples;
            latencyHistograms[i] = statistics.latencyHistogram;
         }
         return new BenchmarkResult(producerThroughput, consumerThroughput, endToEndThroughput, tooHighSamples, TimeUnit.SECONDS, latencyHistograms, outputValueUnitScalingRatio, TimeUnit.MICROSECONDS);
      }

      public static BenchmarkResult merge(BenchmarkResult[] results, int totalRuns) {
         final long[] producerThroughputs = new long[totalRuns];
         final long[] consumerThroughputs = new long[totalRuns];
         final long[] endToEndThroughputs = new long[totalRuns];
         final long[] tooHighSamples = new long[totalRuns];
         final TimeUnit throughputTimeUnit = results[0].throughputTimeUnit;
         final Histogram[] latencyHistograms = new Histogram[totalRuns];
         final double outputValueUnitScalingRatio = results[0].outputValueUnitScalingRatio;
         final TimeUnit latencyTimeUnit = results[0].latencyTimeUnit;
         for (int r = 0; r < totalRuns; r++) {
            long producerThroughput = 0;
            long consumerThroughput = 0;
            long endToEndThroughput = 0;
            long tooHighSample = 0;
            final Histogram latencyHistogram = new Histogram(2);
            long maxDuration = Long.MIN_VALUE;
            for (int f = 0; f < results.length; f++) {
               final BenchmarkResult result = results[f];
               producerThroughput += result.producerThroughput[r];
               consumerThroughput += result.consumerThroughput[r];
               endToEndThroughput += result.endToEndThroughput[r];
               tooHighSample += result.tooHighSamples[r];
               final Histogram histogram = result.latencyHistograms[r];
               latencyHistogram.add(histogram);
               final long duration = histogram.getEndTimeStamp() - histogram.getStartTimeStamp();
               maxDuration = Math.max(maxDuration, duration);
               //System.out.println("[" + (f + 1) + "] - " + new Date(histogram.getStartTimeStamp()) + " - " + (r == 0 ? "warmup" : ("run " + r)) + " duration:\t" + duration + " ms");
            }
            System.out.println((r == 0 ? "WARMUP" : ("RUN " + r)) + " MAX DURATION: " + maxDuration + " ms");
            producerThroughputs[r] = producerThroughput;
            consumerThroughputs[r] = consumerThroughput;
            endToEndThroughputs[r] = endToEndThroughput;
            latencyHistograms[r] = latencyHistogram;
            tooHighSamples[r] = tooHighSample;
         }
         final JmsMessageHistogramLatencyRecorder.BenchmarkResult benchmarkResult = new JmsMessageHistogramLatencyRecorder.BenchmarkResult(producerThroughputs, consumerThroughputs, endToEndThroughputs, tooHighSamples, throughputTimeUnit, latencyHistograms, outputValueUnitScalingRatio, latencyTimeUnit);
         return benchmarkResult;
      }

      public void print(PrintStream log, OutputFormat outputFormat) {
         for (int i = 0; i < this.latencyHistograms.length; i++) {
            log.println("**************");
            if (i == 0) {
               log.println("WARMUP");
            } else {
               log.println("RUN " + i);
            }
            log.println("**************");
            log.println("Producer Throughput: " + this.producerThroughput[i] + " ops/sec");
            log.println("Consumer Throughput: " + this.consumerThroughput[i] + " ops/sec");
            log.println("EndToEnd Throughput: " + this.endToEndThroughput[i] + " ops/sec");
            if (this.tooHighSamples[i] > 0) {
               log.println("Samples too high: " + this.tooHighSamples[i]);
            }
            log.println("EndToEnd SERVICE-TIME Latencies distribution in " + TimeUnit.MICROSECONDS);
            outputFormat.output(latencyHistograms[i], log, this.outputValueUnitScalingRatio);
         }
      }
   }

   private static final class RunStatistics {

      private final int expectedMessages;
      private long producerStart;
      private long producerEnd;
      private long consumerStart;
      private long consumerEnd;
      private int messages;
      private final Histogram latencyHistogram;
      private final TimeProvider timeProvider;
      private long tooHighSamples;

      public RunStatistics(int expectedMessages, Histogram latencyHistogram, TimeProvider timeProvider) {
         this.expectedMessages = expectedMessages;
         this.latencyHistogram = latencyHistogram;
         this.timeProvider = timeProvider;
         this.messages = 0;
         this.tooHighSamples = 0;
      }

      private boolean onMessage(final long produceTime) {
         if (messages >= expectedMessages) {
            throw new IllegalStateException("this run is full");
         }
         final long consumeTime = timeProvider.now();
         final long elapsedTime = consumeTime - produceTime;
         final long normalizedElapsedTime;
         if (elapsedTime > latencyHistogram.getHighestTrackableValue()) {
            normalizedElapsedTime = latencyHistogram.getHighestTrackableValue();
            this.tooHighSamples++;
         } else {
            normalizedElapsedTime = elapsedTime;
         }
         latencyHistogram.recordValue(normalizedElapsedTime);
         if (messages == 0) {
            if (timeProvider.timeUnit() == TimeUnit.MILLISECONDS) {
               latencyHistogram.setStartTimeStamp(produceTime);
            } else {
               final long produceMillis = System.currentTimeMillis() - TimeUnit.NANOSECONDS.toMillis(elapsedTime);
               latencyHistogram.setStartTimeStamp(produceMillis);
            }
            this.producerStart = produceTime;
            this.consumerStart = consumeTime;
         }
         final boolean lastMessage = messages == expectedMessages - 1;
         if (lastMessage) {
            latencyHistogram.setEndTimeStamp(System.currentTimeMillis());
            this.producerEnd = produceTime;
            this.consumerEnd = consumeTime;
         }
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

   private final ByteBuffer contentBuffer;
   private final RunStatistics[] runStatistics;
   private int currentRun;
   private double outputValueUnitScalingRatio;
   private final Consumer<? super BenchmarkResult> onResult;
   private CountDownLatch[] runFinished;

   public JmsMessageHistogramLatencyRecorder(TimeProvider timeProvider,
                                             int warmup,
                                             int runs,
                                             int iterations,
                                             ByteBuffer heapContentBuffer,
                                             CountDownLatch[] runFinished,
                                             Consumer<? super BenchmarkResult> onResult) {
      this.onResult = onResult;
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
      this.runFinished = runFinished;
   }

   @Override
   public void onMessage(Message message) {
      final long startTime = BytesMessageUtil.decodeTimestamp((BytesMessage) message, contentBuffer);
      //the first message arrived need to mark the current run
      if (!this.runStatistics[currentRun].onMessage(startTime)) {
         this.runFinished[currentRun].countDown();
         currentRun++;
      }
   }

   @Override
   public void close() {
      final BenchmarkResult benchmarkResult = BenchmarkResult.extract(runStatistics, this.outputValueUnitScalingRatio);
      onResult.accept(benchmarkResult);
   }
}
