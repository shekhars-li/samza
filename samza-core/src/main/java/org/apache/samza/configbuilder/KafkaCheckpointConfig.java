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

import org.apache.samza.SamzaException;

import java.util.Map;

/**
 * This class encapsulates all configuration relevant to KafkaCheckpointManager. That is, all these configurations are
 * relevant, iff task.checkpoint.factory is KafkaCheckpointManagerFactory.
 */
public class KafkaCheckpointConfig extends CheckpointConfig {
  private static final String KAFKA_CHECKPOINT_MANAGER_FACTORY =
      "org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory";
  private static final int DEFAULT_CHECKPOINT_REPLICATION_FACTOR = 3;
  private static final int DEFAULT_CHECKPOINT_SEGMENT_BYTES = 26214400;

  private final KafkaSystemConfig systemConfig;
  private final int replicationFactor;
  private final int segmentBytes;

  public KafkaCheckpointConfig(KafkaSystemConfig systemConfig) {
    this(systemConfig, DEFAULT_CHECKPOINT_REPLICATION_FACTOR, DEFAULT_CHECKPOINT_SEGMENT_BYTES);
  }

  public KafkaCheckpointConfig(KafkaSystemConfig systemConfig, int replicationFactor) {
    this(systemConfig, replicationFactor, DEFAULT_CHECKPOINT_SEGMENT_BYTES);
  }

  /**
   * Returns KafkaCheckpointConfig that encapsulates all relevant configs for KafkaCheckpointManager
   * For example, segmentBytes and replicationFactor are not relevant when checkpointManager != KafkaCheckpointManager
   *
   * @param systemConfig An instance of KafkaSystemConfig that encapsulates all configurations of the kafka system that
   *                     will be used by KafkaCheckpointManager
   * @param replicationFactor Number of replicas for the Kafka checkpoint topic
   * @param segmentBytes Segment size to use for the Kafka checkpoint topic
   */
  public KafkaCheckpointConfig(KafkaSystemConfig systemConfig, int replicationFactor, int segmentBytes) {
    super(KAFKA_CHECKPOINT_MANAGER_FACTORY);
    if (systemConfig == null) {
      throw new SamzaException("KafkaSystemConfig cannot be null when defining KafkaCheckpointConfig!");
    }

    this.systemConfig = systemConfig;
    this.replicationFactor = replicationFactor;
    this.segmentBytes = segmentBytes;
  }

  @Override
  public Map<String, String> build() {
    Map<String, String> result = super.build();

    result.putAll(systemConfig.build());
    result.put("task.checkpoint.system", systemConfig.systemName);
    result.put("task.checkpoint.replication.factor", String.valueOf(replicationFactor));
    result.put("task.checkpoint.segment.bytes", String.valueOf(segmentBytes));

    return result;
  }
}
