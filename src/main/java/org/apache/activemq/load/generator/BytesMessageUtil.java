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

import javax.jms.BytesMessage;
import java.nio.ByteBuffer;

public class BytesMessageUtil {

   private static final int MESSAGE_TIMESTAMP_OFFSET = 0;

   public static void encodeTimestamp(BytesMessage bytesMessage, ByteBuffer contentArrayBytes, long timestamp) {
      contentArrayBytes.putLong(MESSAGE_TIMESTAMP_OFFSET, timestamp);
      try {
         bytesMessage.writeBytes(contentArrayBytes.array());
      } catch (Exception e) {
         throw new IllegalStateException(e);
      }
   }

   public static long decodeTimestamp(BytesMessage bytesMessage, ByteBuffer contentArrayBytes) {
      try {
         bytesMessage.readBytes(contentArrayBytes.array());
         return contentArrayBytes.getLong(MESSAGE_TIMESTAMP_OFFSET);
      } catch (Exception e) {
         throw new IllegalStateException(e);
      }
   }
}
