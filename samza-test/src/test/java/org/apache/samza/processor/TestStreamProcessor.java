/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.processor;

import org.apache.samza.configbuilder.ConfigBuilder;
import org.apache.samza.configbuilder.KafkaCheckpointConfig;
import org.apache.samza.configbuilder.KafkaSystemConfig;
import org.apache.samza.configbuilder.SerdeConfig;
import org.apache.samza.metrics.MetricsReporter;

import java.util.HashMap;

public class TestStreamProcessor {
  public static void main(String[] args) {
    KafkaSystemConfig kafkaSystemConfig = new KafkaSystemConfig("kafka-system", "localhost:2181", "localhost:9092");
    KafkaSystemConfig checkpointSystemConfig = new KafkaSystemConfig("checkpoint-system", "localhost:2181", "localhost:9092");
    SerdeConfig stringSerde = new SerdeConfig("string", SerdeConfig.SerdeAlias.STRING);

    ConfigBuilder builder = ConfigBuilder.getStandaloneConfigBuilder("test-job", "org.apache.samza.processor.MyStreamTask")
        .addTaskInput(kafkaSystemConfig, "numbers", stringSerde, stringSerde)
        .setCheckpointConfig(new KafkaCheckpointConfig(checkpointSystemConfig, 1));

    final StreamProcessor processor = new StreamProcessor(1, builder.build(), new HashMap<String, MetricsReporter>());
    processor.start();
    try {
      Thread.sleep(30000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    processor.stop();
    System.out.println("Exiting main thread.. Bye!");
  }
}
