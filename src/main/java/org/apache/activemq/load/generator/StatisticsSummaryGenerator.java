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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;

public class StatisticsSummaryGenerator {

   public static void main(String[] args) throws IOException {
      String inputPath = null;
      boolean askedForHelp = false;
      int runs = 5;
      int iterations = 0;
      int warmupIterations = 0;
      boolean producer = false;
      TimeProvider timeProvider = TimeProvider.Nano;
      OutputFormat outputFormat = OutputFormat.DETAIL;
      int targetThroughput = 0;
      for (int i = 0; i < args.length; ++i) {
         final String arg = args[i];
         switch (arg) {
            case "--producer":
               producer = true;
               break;
            case "--help":
               askedForHelp = true;
               break;
            case "--input":
               inputPath = args[++i];
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
            case "--target":
               targetThroughput = Integer.parseInt(args[++i]);
               break;
            case "--time":
               timeProvider = TimeProvider.valueOf(args[++i]);
               break;
            case "--format":
               outputFormat = OutputFormat.valueOf(args[++i]);
               break;
            default:
               throw new AssertionError("Invalid args: " + args[i] + " try --help");
         }
      }
      if (askedForHelp) {
         final String validArgs = "\"[--producer] --target targetThroughput --input inputFileName --warmup warmupIterations --runs runs --iterations iterations [--time Nano|Millis] [--format LONG|SHORT|DETAIL]\"";
         System.err.println("valid arguments = " + validArgs);
         if (args.length == 1) {
            return;
         }
      }
      final PrintStream out = System.out;
      final TimeUnit outputTimeUnit = TimeUnit.MICROSECONDS;
      final long oneSecInProvidedTimeUnit = timeProvider.timeUnit().convert(1, TimeUnit.SECONDS);
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
      final Histogram serviceTimeHistogram = new Histogram(2);
      final Histogram responseTimeHistogram;
      if (targetThroughput > 0) {
         responseTimeHistogram = new Histogram(2);
      } else {
         responseTimeHistogram = null;
      }
      try (StatisticsReader reader = new StatisticsReader(new File(inputPath))) {
         final StatisticsReader.Sample sample = new StatisticsReader.Sample();
         out.println("********************\tRESULTS OF WARM-UP\t********************");
         if (producer) {
            printProducerSummary(reader, out, warmupIterations, serviceTimeHistogram, sample, oneSecInProvidedTimeUnit, outputValueUnitScalingRatio, outputTimeUnit, outputFormat);
         } else {
            printEndToEndSummary(reader, out, warmupIterations, serviceTimeHistogram, 0, null, sample, oneSecInProvidedTimeUnit, outputValueUnitScalingRatio, outputTimeUnit, outputFormat);
         }
         out.println("********************\tEND RESULTS OF WARM-UP\t********************");
         for (int r = 0; r < runs; r++) {
            final int runNumber = (r + 1);
            out.println("********************\tRESULTS OF RUN " + runNumber + "\t********************");
            if (producer) {
               printProducerSummary(reader, out, iterations, serviceTimeHistogram, sample, oneSecInProvidedTimeUnit, outputValueUnitScalingRatio, outputTimeUnit, outputFormat);
            } else {
               printEndToEndSummary(reader, out, iterations, serviceTimeHistogram, targetThroughput, responseTimeHistogram, sample, oneSecInProvidedTimeUnit, outputValueUnitScalingRatio, outputTimeUnit, outputFormat);
            }
            out.println("********************\tEND RESULTS OF RUN " + runNumber + "\t********************");
         }
      }
   }

   public static void printEndToEndSummary(StatisticsReader reader,
                                           PrintStream out,
                                           int iterations,
                                           Histogram serviceTimeHistogram,
                                           int targetThroughput,
                                           Histogram responseTimeHistogram,
                                           StatisticsReader.Sample sample,
                                           long oneSecInProvidedTimeUnit,
                                           double outputScalingRatio,
                                           TimeUnit outputTimeUnit,
                                           OutputFormat outputFormat) {
      final long targetPeriod;
      if (targetThroughput > 0) {
         targetPeriod = oneSecInProvidedTimeUnit / targetThroughput;
      } else {
         targetPeriod = 0;
      }
      long startProducer = 0;
      long startConsumer = 0;
      long endProducer = 0;
      long endConsumer = 0;
      serviceTimeHistogram.reset();
      if (responseTimeHistogram != null) {
         responseTimeHistogram.reset();
      }
      for (int m = 0; m < iterations; m++) {
         if (!reader.readUsing(sample)) {
            throw new IllegalStateException("unexpected EOF!");
         }
         final long value = sample.value();
         if (m == 0) {
            final long time = sample.time();
            startProducer = time;
            startConsumer = time + value;
         } else if (m == (iterations - 1)) {
            final long time = sample.time();
            endProducer = time;
            endConsumer = time + value;
         }
         if (responseTimeHistogram != null) {
            final long intendedStart = startProducer + (m * targetPeriod);
            final long waitTime = sample.time() - intendedStart;
            final long serviceTime = sample.value();
            assert serviceTime > 0 : "service time must be > 0";
            if (waitTime < 0) {
               System.err.println("waitTime = " + waitTime + " turned to be 0 due to approximation errors");
               responseTimeHistogram.recordValue(serviceTime);
            } else {
               final long responseTime = waitTime + serviceTime;
               responseTimeHistogram.recordValue(responseTime);
            }
         }
         serviceTimeHistogram.recordValue(value);
      }
      final long elapsedProducer = endProducer - startProducer;
      final long elapsedConsumer = endConsumer - startConsumer;
      final long elapsedEndToEnd = endConsumer - startProducer;
      final long tptProducerPerSec = (iterations * oneSecInProvidedTimeUnit) / elapsedProducer;
      final long tptConsumerPerSec = (iterations * oneSecInProvidedTimeUnit) / elapsedConsumer;
      final long tptEndToEndPerSec = (iterations * oneSecInProvidedTimeUnit) / elapsedEndToEnd;
      out.printf("Producer elapsed time: %.3f seconds\n", (double) elapsedProducer / oneSecInProvidedTimeUnit);
      out.printf("Consumer elapsed time: %.3f seconds\n", (double) elapsedConsumer / oneSecInProvidedTimeUnit);
      out.printf("EndToEnd elapsed time: %.3f seconds\n", (double) elapsedEndToEnd / oneSecInProvidedTimeUnit);
      if (targetThroughput > 0) {
         out.println("Target Throughput: " + targetThroughput + " ops/sec");
      }
      out.println("Producer Throughput: " + tptProducerPerSec + " ops/sec");
      out.println("Consumer Throughput: " + tptConsumerPerSec + " ops/sec");
      out.println("EndToEnd Throughput: " + tptEndToEndPerSec + " ops/sec");
      out.println("EndToEnd SERVICE-TIME Latencies distribution in " + outputTimeUnit);
      outputFormat.output(serviceTimeHistogram, out, outputScalingRatio);
      if (responseTimeHistogram != null) {
         out.println("EndToEnd RESPONSE-TIME Latencies distribution in " + outputTimeUnit);
         outputFormat.output(responseTimeHistogram, out, outputScalingRatio);
      }
   }

   public static void printProducerSummary(StatisticsReader reader,
                                           PrintStream out,
                                           int iterations,
                                           Histogram histogram,
                                           StatisticsReader.Sample sample,
                                           long oneSecInProvidedTimeUnit,
                                           double outputScalingRatio,
                                           TimeUnit outputTimeUnit,
                                           OutputFormat outputFormat) {
      long startProducer = 0;
      long endProducer = 0;
      histogram.reset();
      for (int m = 0; m < iterations; m++) {
         if (!reader.readUsing(sample)) {
            throw new IllegalStateException("unexpected EOF!");
         }
         final long value = sample.value();
         if (m == 0) {
            final long time = sample.time();
            startProducer = time;
         } else if (m + 1 == iterations) {
            final long time = sample.time();
            endProducer = time;
         }
         histogram.recordValue(value);
      }
      final long elapsedProducer = endProducer - startProducer;
      final long tptProducerPerSec = (iterations * oneSecInProvidedTimeUnit) / elapsedProducer;
      out.printf("Producer elapsed time: %.3f seconds\n", (double) elapsedProducer / oneSecInProvidedTimeUnit);
      out.println("Producer Throughput: " + tptProducerPerSec + " ops/sec");
      out.println("Producer Latencies distribution in " + outputTimeUnit);
      outputFormat.output(histogram, out, outputScalingRatio);
   }
}
