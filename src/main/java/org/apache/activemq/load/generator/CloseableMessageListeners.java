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
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Consumer;

final class CloseableMessageListeners {

   private CloseableMessageListeners() {

   }

   public static CloseableMessageListener with(DestinationBench.BenchmarkConfiguration conf,
                                               Consumer<? super JmsMessageHistogramLatencyRecorder.BenchmarkResult> onResult) throws FileNotFoundException {
      final CloseableMessageListener messageListener;
      switch (conf.sampleMode) {
         case LossLess: {
            final long messages = conf.messages;
            final ByteBuffer consumerBuffer = ByteBuffer.allocate(conf.messageBytes).order(ByteOrder.nativeOrder());
            messageListener = new JmsMessageLossLessLatencyRecorder(conf.outputFile, conf.timeProvider, messages, consumerBuffer);
         }
         break;
         case Percentile: {
            final ByteBuffer consumerBuffer = ByteBuffer.allocate(conf.messageBytes).order(ByteOrder.nativeOrder());
            PrintStream log = null;
            if (conf.outputFile != null) {
               log = new PrintStream(new FileOutputStream(conf.outputFile));
            }
            messageListener = new JmsMessageHistogramLatencyRecorder(conf.timeProvider, conf.warmupIterations, conf.runs, conf.iterations, consumerBuffer, onResult);
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
