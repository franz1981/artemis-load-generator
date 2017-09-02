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

import javax.jms.JMSException;
import java.util.ArrayDeque;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.utils.collections.LinkedListImpl;
import org.jctools.queues.SpscUnboundedArrayQueue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.LinuxPerfProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class QueueBenchmark {

   private enum QueueType {
      JcTool, ChunkedQueue, ArrayDeque, JcLinkedList, ArtemisLinkedList
   }

   private static final Long VALUE = 0L;
   @Param({"JcTool", "ChunkedQueue", "ArrayDeque", "JcLinkedList", "ArtemisLinkedList"})
   public String qType;

   public int chunkSize = 1024;
   private QueueAdapter<Long> q;

   @Setup(Level.Iteration)
   public void init() throws JMSException {
      final QueueType queueType = QueueType.valueOf(qType);
      switch (queueType) {
         case JcLinkedList:
            q = QueueAdapter.jcLinkedList(new LinkedList<>());
            break;
         case ArtemisLinkedList:
            q = QueueAdapter.artemisLinkedList(new LinkedListImpl<>());
            break;
         case ChunkedQueue:
            q = QueueAdapter.chunkedQueue(ChunkedQueue.with(chunkSize));
            break;
         case ArrayDeque:
            q = QueueAdapter.jcArrayDeque(new ArrayDeque<>(chunkSize));
            break;
         case JcTool:
            q = QueueAdapter.spscUnboundedArrayQueue(new SpscUnboundedArrayQueue<>(chunkSize));
      }
   }

   @Benchmark
   @Warmup(iterations = 5, batchSize = 100_000)
   @Measurement(iterations = 5, batchSize = 100_000)
   @OperationsPerInvocation(100_000)
   public Object offer() {
      q.offer(VALUE);
      return q;
   }

   @TearDown(Level.Iteration)
   public void clean() {
      q.clear();
   }

   public static void main(String[] args) throws RunnerException {
      final Options opt = new OptionsBuilder().shouldDoGC(true).include(QueueBenchmark.class.getSimpleName()).addProfiler(LinuxPerfProfiler.class).warmupIterations(5).measurementIterations(5).forks(1).build();
      new Runner(opt).run();
   }

}
