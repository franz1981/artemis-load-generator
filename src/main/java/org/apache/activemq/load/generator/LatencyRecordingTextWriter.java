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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class LatencyRecordingTextWriter {

   public static void main(String[] args) throws IOException {
      String inputPath = null;
      boolean normalized = false;
      boolean askedForHelp = false;
      TimeUnit tptTimeUnit = null;
      for (int i = 0; i < args.length; ++i) {
         final String arg = args[i];
         switch (arg) {
            case "--help":
               askedForHelp = true;
               break;
            case "--input":
               inputPath = args[++i];
               break;
            case "--tpt":
               String tptParam = args[++i];
               switch (tptParam) {
                  case "ns":
                     tptTimeUnit = TimeUnit.NANOSECONDS;
                     break;
                  case "us":
                     tptTimeUnit = TimeUnit.MICROSECONDS;
                     break;
                  case "ms":
                     tptTimeUnit = TimeUnit.MILLISECONDS;
                     break;
                  case "s":
                     tptTimeUnit = TimeUnit.SECONDS;
                     break;
                  default:
                     throw new IllegalArgumentException("--tpt ns|ms|us|s");
               }
               break;
            case "--normalized":
               normalized = true;
               break;
            default:
               throw new AssertionError("Invalid args: " + args[i] + " try --help");
         }
      }
      if (askedForHelp) {
         final String validArgs = "\"--input inputFileName [--normalized] [--tpt ns|ms|us|s]\"";
         System.err.println("valid arguments = " + validArgs);
         if (args.length == 1) {
            return;
         }
      }
      final char endOfLine = '\n';
      final char separator = ',';
      try (StatisticsReader reader = new StatisticsReader(new File(inputPath))) {
         final SampleCsvWriter sampleWriter = new SampleCsvWriter(System.out, separator, endOfLine, tptTimeUnit, normalized);
         final StatisticsReader.Sample sample = new StatisticsReader.Sample();
         while (reader.readUsing(sample)) {
            sampleWriter.write(sample.time(), sample.value());
         }
      }
   }

}
