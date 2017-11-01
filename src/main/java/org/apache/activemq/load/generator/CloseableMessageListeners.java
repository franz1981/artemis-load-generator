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

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.function.Consumer;

final class CloseableMessageListeners {

   private CloseableMessageListeners() {

   }

   public static CloseableMessageListener with(DestinationBench.BenchmarkConfiguration conf,
                                               CyclicBarrier runFinished,
                                               Consumer<? super JmsMessageHistogramLatencyRecorder.BenchmarkResult> onResult) throws FileNotFoundException {
      final CloseableMessageListener messageListener;
      switch (conf.sampleMode) {
         case LossLess: {
            //it makes sense only with single fork cases!
            if (conf.forks > 1) {
               throw new IllegalStateException(SampleMode.LossLess + " smapleMode doesn't support forked executions!");
            }
            final long messages = conf.messages;
            final ByteBuffer consumerBuffer = ByteBuffer.allocate(conf.messageBytes).order(ByteOrder.nativeOrder());
            messageListener = new JmsMessageLossLessLatencyRecorder(conf.outputFile, conf.timeProvider, messages, consumerBuffer);
         }
         break;
         case Percentile: {
            final ByteBuffer consumerBuffer = ByteBuffer.allocate(conf.messageBytes).order(ByteOrder.nativeOrder());
            messageListener = new JmsMessageHistogramLatencyRecorder(conf.timeProvider, conf.warmupIterations, conf.runs, conf.iterations, consumerBuffer, runFinished, onResult);
         }
         break;
         case None:
            messageListener = CloseableMessageListener.blackHole();
            break;
         default:
            throw new AssertionError("unsupported case!");
      }
      return messageListener;
   }
}
