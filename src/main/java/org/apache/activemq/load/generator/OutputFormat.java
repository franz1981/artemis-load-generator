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

import org.HdrHistogram.Histogram;

import static java.lang.String.format;

public enum OutputFormat {
   LONG {
      @Override
      public void output(final Histogram histogram, PrintStream out, double outputScalingRatio) {
         out.append(format("%-6s%20.2f%n", "mean", histogram.getMean()/outputScalingRatio));
         out.append(format("%-6s%20.2f%n", "min", histogram.getMinValue()/outputScalingRatio));
         out.append(format("%-6s%20.2f%n", "50.00%", histogram.getValueAtPercentile(50.0d)/outputScalingRatio));
         out.append(format("%-6s%20.2f%n", "90.00%", histogram.getValueAtPercentile(90.0d)/outputScalingRatio));
         out.append(format("%-6s%20.2f%n", "99.00%", histogram.getValueAtPercentile(99.0d)/outputScalingRatio));
         out.append(format("%-6s%20.2f%n", "99.90%", histogram.getValueAtPercentile(99.9d)/outputScalingRatio));
         out.append(format("%-6s%20.2f%n", "99.99%", histogram.getValueAtPercentile(99.99d)/outputScalingRatio));
         out.append(format("%-6s%20.2f%n", "max", histogram.getMaxValue()/outputScalingRatio));
         out.append(format("%-6s%20d%n", "count", histogram.getTotalCount()));
         out.append("\n");
         out.flush();
      }
   }, SHORT {
      @Override
      public void output(final Histogram histogram, final PrintStream out, double outputScalingRatio) {
         out.append(format("%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%d%n",
                           histogram.getMean()/outputScalingRatio,
                           histogram.getMinValue()/outputScalingRatio,
                           histogram.getValueAtPercentile(50.0d)/outputScalingRatio,
                           histogram.getValueAtPercentile(90.0d)/outputScalingRatio,
                           histogram.getValueAtPercentile(99.0d)/outputScalingRatio,
                           histogram.getValueAtPercentile(99.9d)/outputScalingRatio,
                           histogram.getValueAtPercentile(99.99d)/outputScalingRatio,
                           histogram.getMaxValue()/outputScalingRatio,
                           histogram.getTotalCount()));
         out.flush();
      }
   }, DETAIL {
      @Override
      public void output(final Histogram histogram, final PrintStream out, double outputScalingRatio) {
         histogram.outputPercentileDistribution(out, outputScalingRatio);
      }
   };

   public abstract void output(final Histogram histogram, final PrintStream out, double outputScalingRatio);
}
