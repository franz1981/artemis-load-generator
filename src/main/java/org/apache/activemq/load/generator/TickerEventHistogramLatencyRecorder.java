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
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;

final class TickerEventHistogramLatencyRecorder implements CloseableTickerEventListener {

   private final File outputFile;
   private final long[] elapsedTimes;
   private final Histogram[] responseTimeHistograms;
   private final Histogram[] waitTimeHistograms;
   private final Histogram[] serviceTimeHistograms;
   private final long highestTrackableTimeInNanos;

   private final Histogram responseTimeHistogram;
   private final Histogram waitTimeHistogram;
   private final Histogram serviceTimeHistogram;
   private long startTime;

   TickerEventHistogramLatencyRecorder(final File outputFile, final int runs, final long highestTrackableTimeInNanos) {
      this.highestTrackableTimeInNanos = highestTrackableTimeInNanos;
      this.outputFile = outputFile;
      this.elapsedTimes = new long[runs + 1];
      this.responseTimeHistogram = new Histogram(highestTrackableTimeInNanos, 2);
      this.waitTimeHistogram = new Histogram(highestTrackableTimeInNanos, 2);
      this.serviceTimeHistogram = new Histogram(highestTrackableTimeInNanos, 2);
      this.responseTimeHistograms = new Histogram[runs+1];
      this.waitTimeHistograms = new Histogram[runs+1];
      this.serviceTimeHistograms = new Histogram[runs+1];
      for(int i = 0;i<(runs+1);i++){
         this.serviceTimeHistograms[i] = serviceTimeHistogram.copy();
         this.waitTimeHistograms[i] = waitTimeHistogram.copy();
         this.responseTimeHistograms[i] = responseTimeHistogram.copy();
      }
   }


   private static long min(long a,long b){
      //isATheMinimum==ALL 1s <-> min(a,b)==a && isATheMinimum==0 <-> min(a,b)==b
      final long isATheMinimum = (a-b)>>63;
      final long min = (a&isATheMinimum)|(b&(~isATheMinimum));
      return min;
   }

   @Override
   public void onStartedRun(int run) {
      startTime = System.nanoTime();
   }



   @Override
   public void onServiceTimeSample(int run, long time, long serviceTime) {
      serviceTimeHistogram.recordValue(min(serviceTime, highestTrackableTimeInNanos));
   }

   @Override
   public void onResponseTimeSample(int run, long time, long responseTime) {
      responseTimeHistogram.recordValue(min(responseTime, highestTrackableTimeInNanos));
   }

   @Override
   public void onWaitTimeSample(int run, long time, long waitTime) {
      waitTimeHistogram.recordValue(min(waitTime, highestTrackableTimeInNanos));
   }

   @Override
   public void onFinishedRun(int run) {
      final long endTime = System.nanoTime();
      elapsedTimes[run] = endTime - startTime;
      try{
         serviceTimeHistogram.copyInto(serviceTimeHistograms[run]);
         waitTimeHistogram.copyInto(waitTimeHistograms[run]);
         responseTimeHistogram.copyInto(responseTimeHistograms[run]);
      }finally{
         serviceTimeHistogram.reset();
         waitTimeHistogram.reset();
         responseTimeHistogram.reset();
      }
   }

   @Override
   public void close() {
      try (PrintStream out = new PrintStream(outputFile)) {
         for (int i = 0; i < elapsedTimes.length; i++) {
            //decode from ByteBuffers
            final Histogram serviceTime = serviceTimeHistograms[i];
            final long iterations = serviceTime.getTotalCount();
            final long elapsedTimeNanos = elapsedTimes[i];
            final long iterationsPerSec = (iterations * 1000_000_000L) / elapsedTimeNanos;
            final long elapsedTimeSeconds = TimeUnit.NANOSECONDS.toSeconds(elapsedTimeNanos);
            out.println("********************\tRESULTS OF RUN " + i + "\t********************");
            out.println("Elapsed in:" + elapsedTimeSeconds + " s");
            out.println("Effective throughout: " + iterationsPerSec + " iterations/sec");
            out.println("SERVICE TIME Latencies distribution in us:");
            serviceTime.outputPercentileDistribution(out, 1000.0);
            out.println("RESPONSE TIME Latencies distribution in us:");
            final Histogram responseTime = responseTimeHistograms[i];
            responseTime.outputPercentileDistribution(out, 1000.0);
            out.println("WAIT TIME Latencies distribution in us:");
            final Histogram waitTime = waitTimeHistograms[i];
            waitTime.outputPercentileDistribution(out, 1000.0);
            out.println("********************\tEND RESULTS OF RUN " + i + "\t********************");
         }
      }
      catch (FileNotFoundException e) {
         throw new IllegalStateException(e);
      }
   }
}
