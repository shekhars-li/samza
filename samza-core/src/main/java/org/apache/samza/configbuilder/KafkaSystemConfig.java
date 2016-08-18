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
package org.apache.samza.configbuilder;

import java.util.HashMap;
import java.util.Map;

public class KafkaSystemConfig extends SystemConfig {
  private static final String KAFKA_SYSTEM_FACTORY = "org.apache.samza.system.kafka.KafkaSystemFactory";
  private static final String ZOOKEEPER_CONNECT_FORMAT_STRING = "systems.%s.consumer.zookeeper.connect";
  private static final String BOOTSTRAP_SERVERS = "systems.%s.producer.bootstrap.servers";
  private static final String KAFKA_SYSTEM_CONSUMER_PROPERTIES_FORMAT_STRING = "systems.%s.consumer.%s";
  private static final String KAFKA_SYSTEM_PRODUCER_PROPERTIES_FORMAT_STRING = "systems.%s.producer.%s";

  public final String zookeeperConnect;
  public final String bootstrapServers;
  public final Map<String, String> consumerProperties = new HashMap<>();
  public final Map<String, String> producerProperties = new HashMap<>();

  public KafkaSystemConfig(
      String systemName,
      String zookeeperConnect,
      String bootstrapServers,
      SerdeConfig keySerde,
      SerdeConfig msgSerde) {
    super(systemName, KAFKA_SYSTEM_FACTORY, keySerde, msgSerde);
    this.zookeeperConnect = zookeeperConnect;
    this.bootstrapServers = bootstrapServers;
  }

  public KafkaSystemConfig(String systemName, String zookeeperConnect, String bootstrapServers) {
    this(systemName, zookeeperConnect, bootstrapServers, null, null);
  }

  @Override
  public Map<String, String> build() {
    Map<String, String> result = super.build();

    // Consumer Properties
    // Mandatory
    result.put(String.format(ZOOKEEPER_CONNECT_FORMAT_STRING, systemName), zookeeperConnect);

    // Optional
    if (consumerProperties.size() > 0) {
      for (Map.Entry<String, String> entry: consumerProperties.entrySet()) {
        result.put(String.format(KAFKA_SYSTEM_CONSUMER_PROPERTIES_FORMAT_STRING, systemName, entry.getKey()), entry.getValue());
      }
    }

    // Producer Properties
    // Mandatory
    result.put(String.format(BOOTSTRAP_SERVERS, systemName), bootstrapServers);

    // Optional
    if (producerProperties.size() > 0) {
      for (Map.Entry<String, String> entry: producerProperties.entrySet()) {
        result.put(String.format(KAFKA_SYSTEM_PRODUCER_PROPERTIES_FORMAT_STRING, systemName, entry.getKey()), entry.getValue());
      }
    }

    return result;
  }
}
