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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;

public class HistogramGenerator {

   public static void main(String[] args) throws IOException {
      boolean forceElapsedNano = false;
      boolean coordinatedOmissionFix = false;
      long targetThroughput = 0;
      String inputPath = null;
      boolean askedForHelp = false;

      for (int i = 0; i < args.length; ++i) {
         final String arg = args[i];
         switch (arg) {
            case "--nano":
               forceElapsedNano = true;
               break;
            case "--target":
               targetThroughput = Long.parseLong(args[++i]);
               break;
            case "--co-fix":
               coordinatedOmissionFix = true;
               break;
            case "--help":
               askedForHelp = true;
               break;
            case "--input":
               inputPath = args[++i];
               break;
            default:
               throw new AssertionError("Invalid args: " + args[i] + " try throughputBenchmark --help");
         }
      }
      if (askedForHelp) {
         final String validArgs = "\"--target targetThroughput --input inputFileName [--nano] --co-fix\"";
         System.err.println("valid arguments = " + validArgs);
         if (args.length == 1) {
            return;
         }
      }
      final char endOfLine = '\n';
      final StringBuilder asciiBuilder = new StringBuilder();
      final long nanoPeriod = TimeUnit.SECONDS.toNanos(1) / targetThroughput;
      final long maxPeriod = Math.max(TimeUnit.SECONDS.toNanos(10), nanoPeriod * 1000000);
      final Histogram histogram = new Histogram(maxPeriod, 2);
      final Histogram coFreeHistogram;
      if (coordinatedOmissionFix) {
         coFreeHistogram = new Histogram(maxPeriod, 2);
      } else {
         coFreeHistogram = null;
      }
      try (final RandomAccessFile raf = new RandomAccessFile(inputPath, "r"); final FileChannel fc = raf.getChannel()) {
         final MappedByteBuffer bytes = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
         final int capacity = bytes.capacity();
         int position = 0;
         while (position < capacity) {
            asciiBuilder.setLength(0);
            char c;
            while (position < capacity && (c = (char) bytes.get(position)) != endOfLine) {
               asciiBuilder.append(c);
               position++;
            }
            position++;
            if (asciiBuilder.length() > 0) {
               final String ascii = asciiBuilder.toString();
               final long elapsedInNanos;
               if (forceElapsedNano) {
                  elapsedInNanos = Long.parseLong(ascii);
               } else {
                  final double value = Double.parseDouble(ascii);
                  elapsedInNanos = (long) (value * 1000_000L);
               }
               histogram.recordValue(elapsedInNanos);
               if (coFreeHistogram != null) {
                  coFreeHistogram.recordValueWithExpectedInterval(elapsedInNanos, nanoPeriod);
               }
            }
         }
      }
      System.out.println("Latencies distribution in microseconds:");
      histogram.outputPercentileDistribution(System.out, 1000.0);
      if (coFreeHistogram != null) {
         System.out.println("Latencies distribution in microseconds (CO FREE):");
         coFreeHistogram.outputPercentileDistribution(System.out, 1000.0);
      }
   }
}
