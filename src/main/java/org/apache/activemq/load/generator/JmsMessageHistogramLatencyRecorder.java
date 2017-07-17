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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;

final class JmsMessageHistogramLatencyRecorder implements CloseableMessageListener {

   private final File outputFile;
   private final TimeProvider timeProvider;
   private final ByteBuffer contentBuffer;
   private final int runs;
   private final Histogram[] histograms;
   private final long[] messagesLimit;
   private final long highestTrackableValue;
   private long messages;
   private int currentIndex;
   private Histogram currentHistogram;
   private long currentMessageLimit;
   private double outputValueUnitScalingRatio;

   public JmsMessageHistogramLatencyRecorder(File outputFile,
                                             TimeProvider timeProvider,
                                             int warmup,
                                             int runs,
                                             int iterations,
                                             ByteBuffer heapContentBuffer) {
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
      this.outputFile = outputFile;
      this.timeProvider = timeProvider;
      this.runs = runs;
      this.histograms = new Histogram[runs + 1];
      this.messagesLimit = new long[runs + 1];
      this.highestTrackableValue = timeProvider.timeUnit().convert(10, TimeUnit.SECONDS);
      for (int i = 0; i < histograms.length; i++) {
         final Histogram histogram = new Histogram(highestTrackableValue, 2);
         histograms[i] = histogram;
         messagesLimit[i] = warmup + (i * iterations);
      }
      this.messages = 0;
      this.contentBuffer = heapContentBuffer;
      if (!contentBuffer.hasArray()) {
         throw new IllegalArgumentException("content buffer must be on heap and writable!");
      }
      this.currentIndex = 0;
      this.currentHistogram = histograms[0];
      this.currentMessageLimit = messagesLimit[0];
   }

   private static long min(long a, long b) {
      //isATheMinimum==ALL 1s <-> min(a,b)==a && isATheMinimum==0 <-> min(a,b)==b
      final long isATheMinimum = (a - b) >> 63;
      final long min = (a & isATheMinimum) | (b & (~isATheMinimum));
      return min;
   }

   @Override
   public void onMessage(Message message) {
      final long startTime = BytesMessageUtil.decodeTimestamp((BytesMessage) message, contentBuffer);
      //include the decoding time
      final long elapsedTime = min(timeProvider.now() - startTime, highestTrackableValue);
      assert elapsedTime >= 0 : "time can't flow in the opposite direction";
      if (messages < currentMessageLimit) {
         currentHistogram.recordValue(elapsedTime);
      } else {
         final Histogram histogram = switchHistogram();
         if (histogram != null) {
            histogram.recordValue(elapsedTime);
         }
      }
      messages++;
   }

   private Histogram switchHistogram() {
      if (currentIndex == runs) {
         return null;
      } else {
         currentIndex++;
         currentHistogram = histograms[currentIndex];
         currentMessageLimit = messagesLimit[currentIndex];
         return currentHistogram;
      }
   }

   @Override
   public void close() {
      try (PrintStream outputStream = new PrintStream(outputFile)) {

         for (int i = 0; i < histograms.length; i++) {
            histograms[i].outputPercentileDistribution(outputStream, outputValueUnitScalingRatio);
         }
      } catch (FileNotFoundException e) {
         throw new IllegalStateException(e);
      }
   }
}
