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

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

final class SampleCsvWriter {

   private final PrintStream output;
   private final boolean normalized;
   private final TimeUnit tptTimeUnit;
   private final StringBuilder lineBuilder;
   private final char separator;
   private final char endOfLine;
   private final long factor;
   private long startTimestamp;

   public SampleCsvWriter(PrintStream output, char separator, char endOfLine, TimeUnit tptTimeUnit, boolean normalized) {
      this.output = output;
      this.normalized = normalized;
      this.endOfLine = endOfLine;
      this.separator = separator;
      this.tptTimeUnit = tptTimeUnit;
      this.lineBuilder = new StringBuilder();
      lineBuilder.setLength(0);
      lineBuilder.append("timestamp").append(separator).append("value");
      print(lineBuilder, output);
      lineBuilder.setLength(0);
      this.startTimestamp = -1L;
      if (tptTimeUnit != null) {
         this.factor = tptTimeUnit.convert(1, TimeUnit.SECONDS);
      }
      else {
         this.factor = 1;
      }
   }

   private static void print(CharSequence charSequence, PrintStream dataOutput) {
      for (int i = 0; i < charSequence.length(); i++) {
         dataOutput.write(charSequence.charAt(i));
      }
   }

   public void write(final long time, final long value) {
      if (normalized && startTimestamp == -1L) {
         startTimestamp = time;
      }
      final long timestamp = time - startTimestamp;
      final long latency = value;
      final long valueToPrint;
      if (tptTimeUnit != null) {
         final long throughputPerSeconds = this.factor / latency;
         valueToPrint = throughputPerSeconds;
      }
      else {
         valueToPrint = latency;
      }
      lineBuilder.append(endOfLine);
      lineBuilder.append(timestamp).append(separator).append(valueToPrint);
      print(lineBuilder, this.output);
      lineBuilder.setLength(0);
   }
}