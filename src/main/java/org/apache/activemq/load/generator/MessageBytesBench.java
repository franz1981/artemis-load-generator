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
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Hashtable;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Thread)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class MessageBytesBench {

   private static final int MESSAGE_BODY_SIZE = 100;

   private static ConnectionFactory connectionFactory;
   private static int messageBodySize;

   private Session session;
   private Connection connection;
   private BytesMessage message;
   private ByteBuffer messageContent;

   @Setup
   public void init() throws JMSException {
      this.connection = connectionFactory.createConnection();
      this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      this.message = session.createBytesMessage();
      this.messageContent = ByteBuffer.allocate(MESSAGE_BODY_SIZE).order(ByteOrder.nativeOrder());
      this.message.clearBody();
      BytesMessageUtil.encodeTimestamp(message, messageContent, System.nanoTime());
   }

   @Benchmark
   public BytesMessage encode() throws Exception {
      this.message.clearBody();
      BytesMessageUtil.encodeTimestamp(message, messageContent, System.nanoTime());
      return message;
   }

   @Benchmark
   public long decode() throws JMSException {
      this.message.reset();
      return BytesMessageUtil.decodeTimestamp(message, messageContent);
   }

   @TearDown
   public void close() throws Exception {
      try {
         this.session.close();
      } finally {
         this.connection.close();
      }
   }

   public static void main(String[] args) throws Exception {
      String url = null;
      int messageBodySize = 100;
      boolean askedForHelp = false;
      for (int i = 0; i < args.length; ++i) {
         final String arg = args[i];
         switch (arg) {
            case "--url":
               url = args[++i];
            case "--bytes":
               messageBodySize = Integer.parseInt(args[++i]);
            case "--help":
               askedForHelp = true;
               break;
            default:
               throw new AssertionError("Invalid args: " + args[i] + " try --help");
         }
      }
      if (askedForHelp) {
         final String validArgs = "\"[--url url] [--bytes messageBytes]\"";
         System.err.println("valid arguments = " + validArgs);
         if (args.length == 1) {
            return;
         }
      }

      final Hashtable<Object, Object> env = new Hashtable<Object, Object>();
      env.put("connectionFactory.ConnectionFactory", url);

      final Context context = new InitialContext(env);
      final ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("ConnectionFactory");
      MessageBytesBench.connectionFactory = connectionFactory;
      MessageBytesBench.messageBodySize = messageBodySize;
      final Options opt = new OptionsBuilder().include(MessageBytesBench.class.getSimpleName()).addProfiler(GCProfiler.class).warmupIterations(5).measurementIterations(5).forks(1).build();
      new Runner(opt).run();
   }

}
