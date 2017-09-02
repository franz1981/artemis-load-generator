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
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

final class CloseableMessageListeners {

   private CloseableMessageListeners() {

   }

   public static CloseableMessageListener with(TimeProvider timeProvider,
                                               int messageBytes,
                                               File consumerStatisticsFile,
                                               SampleMode consumerSampleMode,
                                               OutputFormat latencyFormat,
                                               int iterations,
                                               int runs,
                                               int warmupIterations) throws FileNotFoundException {
      final CloseableMessageListener messageListener;
      switch (consumerSampleMode) {
         case LossLess: {
            final long messages = ((iterations * runs) + warmupIterations);
            final ByteBuffer consumerBuffer = ByteBuffer.allocate(messageBytes).order(ByteOrder.nativeOrder());
            messageListener = new JmsMessageLossLessLatencyRecorder(consumerStatisticsFile, timeProvider, messages, consumerBuffer);
         }
         break;
         case Percentile: {
            final ByteBuffer consumerBuffer = ByteBuffer.allocate(messageBytes).order(ByteOrder.nativeOrder());
            messageListener = new JmsMessageHistogramLatencyRecorder(new PrintStream(new FileOutputStream(consumerStatisticsFile)), latencyFormat, timeProvider, warmupIterations, runs, iterations, consumerBuffer);
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
