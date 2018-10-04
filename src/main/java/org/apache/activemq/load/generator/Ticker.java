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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

final class Ticker implements Runnable {

   private final int targetThroughput;
   private final long nanoPeriod;
   private final int iterations;
   private final int runs;
   private final int warmupIterations;
   private final int waitSecondsBetweenIterations;
   private final ServiceAction service;
   private final TickerEventListener tickerEventListener;
   private final boolean isWaitRate;

   private Ticker(int targetThroughput,
                  long nanoPeriod,
                  int iterations,
                  int runs,
                  int warmupIterations,
                  int waitSecondsBetweenIterations,
                  ServiceAction service,
                  TickerEventListener tickerEventListener,
                  boolean isWaitDelay) {
      this.targetThroughput = targetThroughput;
      this.nanoPeriod = nanoPeriod;
      this.iterations = iterations;
      this.runs = runs;
      this.warmupIterations = warmupIterations;
      this.waitSecondsBetweenIterations = waitSecondsBetweenIterations;
      this.service = service;
      this.tickerEventListener = tickerEventListener;
      this.isWaitRate = isWaitDelay;
   }

   public static Ticker responseUnderLoadBenchmark(final ServiceAction service,
                                                   final TickerEventListener tickerEventListener,
                                                   final int targetThroughtput,
                                                   final int iterations,
                                                   final int runs,
                                                   final int warmupIterations,
                                                   final int waitSecondsBetweenIterations,
                                                   boolean isWaitRate) {
      final long nanoPeriod = targetThroughtput > 0 ? (TimeUnit.SECONDS.toNanos(1) / targetThroughtput) : 0;
      return new Ticker(targetThroughtput, nanoPeriod, iterations, runs, warmupIterations, waitSecondsBetweenIterations, service, tickerEventListener, isWaitRate);
   }

   public static Ticker responseUnderLoadBenchmark(final ServiceAction service,
                                                   final int targetThroughtput,
                                                   final int iterations,
                                                   final int runs,
                                                   final int warmupIterations,
                                                   final int waitSecondsBetweenIterations,
                                                   boolean isWaitRate) {
      final long nanoPeriod = targetThroughtput > 0 ? (TimeUnit.SECONDS.toNanos(1) / targetThroughtput) : 0;
      return new Ticker(targetThroughtput, nanoPeriod, iterations, runs, warmupIterations, waitSecondsBetweenIterations, service, BlackholeTickerEventListener.Instance, isWaitRate);
   }

   public static Ticker throughputBenchmark(final ServiceAction service,
                                            final TickerEventListener tickerEventListener,
                                            final int iterations,
                                            final int runs,
                                            final int warmupIterations,
                                            final int waitSecondsBetweenIterations,
                                            boolean isWaitRate) {
      return Ticker.responseUnderLoadBenchmark(service, tickerEventListener, 0, iterations, runs, warmupIterations, waitSecondsBetweenIterations, isWaitRate);
   }

   public static Ticker throughputBenchmark(final ServiceAction service,
                                            final int iterations,
                                            final int runs,
                                            final int warmupIterations,
                                            final int waitSecondsBetweenIterations,
                                            boolean isWaitRate) {
      return throughputBenchmark(service, BlackholeTickerEventListener.Instance, iterations, runs, warmupIterations, waitSecondsBetweenIterations, isWaitRate);
   }

   public static void spinWaitUntil(long deadline) {
      long waitTime;
      while ((waitTime = (deadline - System.nanoTime())) > 0) {
         LockSupport.parkNanos(waitTime);
      }
   }

   @Override
   public void run() {
      final boolean rulMode = targetThroughput > 0;
      if (rulMode) {
         runRul();
      } else {
         runThroughput();
      }
   }

   private void runThroughput() {
      final int warmupIterations = this.warmupIterations;
      final TickerEventListener tickerEventListener = this.tickerEventListener;
      final ServiceAction service = this.service;
      final long startTest = System.nanoTime();
      tickerEventListener.onStartedRun(0);
      for (int i = 0; i < warmupIterations; i++) {
         throughput(service, tickerEventListener, 0);
      }
      tickerEventListener.onFinishedRun(0);

      final long waitNanosBetweenIterations = TimeUnit.SECONDS.toNanos(this.waitSecondsBetweenIterations);
      final int runs = this.runs;
      final int iterations = this.iterations;
      final boolean isWaitRate = this.isWaitRate;

      for (int r = 0; r < runs; r++) {
         final int run = r + 1;
         if (isWaitRate) {
            final long deadLine = startTest + run * (waitNanosBetweenIterations);
            final long now = System.nanoTime();
            final long waitTime = deadLine - now;
            if (waitTime > 0) {
               LockSupport.parkNanos(waitTime);
            }
         } else {
            LockSupport.parkNanos(waitNanosBetweenIterations);
         }
         tickerEventListener.onStartedRun(run);
         for (int i = 0; i < iterations; i++) {
            throughput(service, tickerEventListener, run);
         }
         tickerEventListener.onFinishedRun(run);
      }
   }

   private void runRul() {
      final int warmupIterations = this.warmupIterations;
      final TickerEventListener tickerEventListener = this.tickerEventListener;
      final ServiceAction service = this.service;
      tickerEventListener.onStartedRun(0);
      final long startTest = System.nanoTime();
      for (int i = 0; i < warmupIterations; i++) {
         final long startServiceTime = System.nanoTime();
         rul(service, tickerEventListener, 0, startServiceTime, startServiceTime);
      }
      tickerEventListener.onFinishedRun(0);

      final long waitNanosBetweenIterations = TimeUnit.SECONDS.toNanos(this.waitSecondsBetweenIterations);
      final int runs = this.runs;
      final int iterations = this.iterations;
      final long nanoPause = this.nanoPeriod;
      final boolean isWaitRate = this.isWaitRate;

      for (int r = 0; r < runs; r++) {
         final int run = r + 1;
         if (isWaitRate) {
            final long deadLine = startTest + run * (waitNanosBetweenIterations);
            final long now = System.nanoTime();
            final long waitTime = deadLine - now;
            if (waitTime > 0) {
               LockSupport.parkNanos(waitTime);
            }
         } else {
            LockSupport.parkNanos(waitNanosBetweenIterations);
         }
         tickerEventListener.onStartedRun(run);
         final long startTime = System.nanoTime();
         long tickTime = startTime;
         for (int i = 0; i < iterations; i++) {
            tickTime += nanoPause;
            spinWaitUntil(tickTime);
            rul(service, tickerEventListener, run, tickTime, System.nanoTime());
         }
         tickerEventListener.onFinishedRun(run);
      }

   }

   private static void rul(ServiceAction service,
                           TickerEventListener tickerEventListener,
                           final int run,
                           final long intendedStartServiceTime,
                           final long startServiceTime) {
      service.doWork(intendedStartServiceTime, startServiceTime);
      final long endServiceTime = System.nanoTime();
      final long serviceTime = endServiceTime - startServiceTime;
      tickerEventListener.onServiceTimeSample(run, startServiceTime, serviceTime);
      final long waitTime = startServiceTime - intendedStartServiceTime;
      tickerEventListener.onWaitTimeSample(run, intendedStartServiceTime, waitTime);
      final long responseTime = endServiceTime - intendedStartServiceTime;
      tickerEventListener.onResponseTimeSample(run, intendedStartServiceTime, responseTime);
   }

   private static void throughput(ServiceAction service, TickerEventListener tickerEventListener, int run) {
      final long startServiceTime = System.nanoTime();
      service.doWork(startServiceTime, startServiceTime);
      final long endServiceTime = System.nanoTime();
      final long serviceTime = endServiceTime - startServiceTime;
      tickerEventListener.onServiceTimeSample(run, startServiceTime, serviceTime);
   }

   public enum BlackholeTickerEventListener implements TickerEventListener {
      Instance;

      @Override
      public void onServiceTimeSample(int run, long time, long serviceTime) {

      }
   }

   @FunctionalInterface
   public interface ServiceAction {

      void doWork(long intendedStartServiceTime, long startServiceTime);
   }

   @FunctionalInterface
   public interface TickerEventListener {

      void onServiceTimeSample(int run, long time, long serviceTime);

      default void onStartedRun(int run) {

      }

      default void onResponseTimeSample(int run, long time, long responseTime) {

      }

      default void onWaitTimeSample(int run, long time, long waitTime) {

      }

      default void onFinishedRun(int run) {

      }
   }

}
