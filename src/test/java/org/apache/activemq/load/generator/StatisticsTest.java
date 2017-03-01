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
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public class StatisticsTest {

   @Test
   public void readsAfterWrites() throws IOException {
      final File statistics = File.createTempFile("statistics", ".tmp");
      statistics.deleteOnExit();
      final long samples = 1_000_000;
      try (StatisticsRecorder statisticsRecorder = new StatisticsRecorder(statistics, samples, true)) {
         for (long i = 0; i < samples; i++) {
            statisticsRecorder.appendSample(i, i);
         }
      }
      try (StatisticsReader statisticsReader = new StatisticsReader(statistics)) {
         final StatisticsReader.Sample sample = new StatisticsReader.Sample();
         for (long i = 0; i < samples; i++) {
            final boolean read = statisticsReader.readUsing(sample);
            Assert.assertTrue(read);
            final long time = sample.time();
            final long value = sample.value();
            Assert.assertEquals(time, value);
            Assert.assertEquals(time, i);
         }
         final boolean read = statisticsReader.readUsing(sample);
         Assert.assertFalse(read);
      }
   }

}
