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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.MapConfig;
import org.apache.samza.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ConfigBuilder {
  // Config Names
  static final String JOB_NAME = "job.name";
  static final String JOB_ID = "job.id";
  static final String DEFAULT_JOB_ID = "1";

  static final String TASK_CLASS = "task.class";
  static final String TASK_INPUTS = "task.inputs";
  static final String JOB_COORDINATOR_FACTORY = "job.coordinator.factory";
  static final String TASK_NAME_GROUPER_FACTORY = "task.name.grouper.factory";
  static final String SSP_GROUPER_FACTORY = "job.systemstreampartition.grouper.factory";

  static final String JOB_CONFIG_REWRITERS = "job.config.rewriters";

  // Mandatory
  private String jobName = null;
  private String taskClass = null;
  private List<SystemStreamConfig> inputList = new ArrayList<>();

  protected String jobCoordinatorFactory;
  protected String sspGrouperFactory;
  protected String taskNameGrouperFactory;

  // Optional
  private String jobId = DEFAULT_JOB_ID;
  private CheckpointConfig checkpointConfig = null;
  private List<SystemConfig> systemConfigs = new ArrayList<>();
  private List<RewriterConfig> rewriterConfigList = new ArrayList<>();

  ConfigBuilder(String jobName, String taskClass) {
    this.jobName = jobName;
    this.taskClass = taskClass;
  }

  /**
   * Returns an instance of StandaloneConfigBuilder
   * StandaloneConfigBuilder has some pre-determined configurations such as sspGrouper, jobCoordinatorFactory etc.
   *
   * @param jobName Name of the Samza Job
   * @param taskClass Name of the class implementing StreamTask. This will be used to dynamically load the class with ClassLoader
   * @return An instance of StandaloneConfigBuilder
   */
  public static StandaloneConfigBuilder getStandaloneConfigBuilder(String jobName, String taskClass) {
    return new StandaloneConfigBuilder(jobName, taskClass);
  }

  /**
   * Returns an instance of GenericConfigBuilder
   * GenericConfigBuilder allows you to configure any available Samza configuration. It is left up to the user to
   * understand the internals of Samza and correctly configure the Job based on the execution environment
   *
   * @param jobName Name of the Samza Job
   * @param taskClass Name of the class implementing StreamTask. This will be used to dynamically load the class with ClassLoader
   * @return An instance of GenericConfigBuilder
   */
  public static GenericConfigBuilder getGenericConfigBuilder(String jobName, String taskClass) {
    return new GenericConfigBuilder(jobName, taskClass);
  }

  /**
   * Sets the value for "job.id" in Samza config
   * job.id is useful to distinguish between multiple instances of the same Samza job.
   *
   * Optional
   * Default Value: "1"
   *
   * @param jobId Value that is usually provided for "job.id"
   * @return this instance of ConfigBuilder
   */
  public ConfigBuilder setJobId(String jobId) {
    this.jobId = jobId;
    return this;
  }

  /**
   * Adds to the list of "task.inputs" in Samza config
   * task.inputs should contain at-least one value for the processor to execute.
   *
   * Required
   *
   * @param systemConfig An instance of SystemConfig
   * @param streamName Name of the stream to consumer as input
   * @param keySerde (Nullable) Serde config for key
   * @param msgSerde (Nullable) Serde config for message
   * @return this instance of ConfigBuilder
   */
  public ConfigBuilder addTaskInput(SystemConfig systemConfig, String streamName, SerdeConfig keySerde, SerdeConfig msgSerde) {
    this.inputList.add(new SystemStreamConfig(systemConfig, streamName, keySerde, msgSerde));
    return this;
  }

  /**
   * Adds a "system" to Samza config.
   *
   * A system refers to a set of properties associated with a given data source/sink. It is usually configured by
   * properties prefixed with "systems.". Typically, every implementation of SystemConfig contains a system name as its
   * identifier.
   *
   * For example, a Kafka system is typically configured in samza with the following list of properties:
   *   - systems.&lt;system-name&gt;.samza.factory = "org.apache.samza.system.kafka.KafkaSystemFactory"
   *   - systems.&lt;system-name&gt;.consumer.zookeeper.connect = "localhost:2181"
   *   - systems.&lt;system-name&gt;.producer.bootstrap.servers = "localhost:9092"
   *
   * There should be at-least one system added to the Samza config which specified the input data source.
   *
   * Required
   *
   * @param systemConfig An instance of SystemConfig implementation
   * @return this instance of ConfigBuilder
   */
  public ConfigBuilder addSystem(SystemConfig systemConfig) {
    this.systemConfigs.add(systemConfig);
    return this;
  }

  /**
   * Sets the configurations required for checkpointing in Samza. It primarily configures the checkpoint systems by
   * setting "task.checkpoint.factory". Other additional properties may be added depending of the implementation of
   * CheckpointConfig used.
   *
   * Setting this property automatically enables checkpointing in a Samza job.
   *
   * Optional
   *
   * @param checkpointConfig
   * @return this instance of ConfigBuilder
   */
  public ConfigBuilder setCheckpointConfig(CheckpointConfig checkpointConfig) {
    this.checkpointConfig = checkpointConfig;
    return this;
  }

  /**
   * Builds the Samze config map and performs some basic validations on the config.
   *
   * @return An instance of Samza Config that defines the semantics of the Samza job
   */
  public Config build() {
    Map<String, String> initialConfig = new HashMap<>();

    if (Strings.isNullOrEmpty(jobName)) {
      throw new ConfigException(String.format("%s is required!", JOB_NAME));
    }
    initialConfig.put(JOB_NAME, jobName);
    initialConfig.put(JOB_ID, jobId);

    if (Strings.isNullOrEmpty(taskClass)) {
      throw new ConfigException("StreamTask is not specified!!");
    }
    initialConfig.put(TASK_CLASS, taskClass);

    if (inputList.size() == 0) {
      throw new ConfigException("Input List should contain at-least 1 SystemStream Config!");
    }

    Joiner taskInputsJoiner = Joiner.on(",").skipNulls();
    String taskInputs = null;
    for (SystemStreamConfig ss: inputList) {
      initialConfig.putAll(ss.build());
      taskInputs = taskInputsJoiner.join(taskInputs, String.format("%s.%s", ss.systemConfig.systemName, ss.streamName));
    }
    initialConfig.put(TASK_INPUTS, taskInputs);

    if (jobCoordinatorFactory == null) {
      throw new ConfigException("Job Coordinator Factory not specified. Cannot proceed!");
    }
    initialConfig.put(JOB_COORDINATOR_FACTORY, jobCoordinatorFactory);

    if (sspGrouperFactory == null) {
      throw new ConfigException("SSPGrouper Factory not specified. Cannot proceed!");
    }
    initialConfig.put(SSP_GROUPER_FACTORY, sspGrouperFactory);

    if (taskNameGrouperFactory == null) {
      throw new ConfigException("TaskName Grouper Factory not specified. Cannot proceed!");
    }
    initialConfig.put(TASK_NAME_GROUPER_FACTORY, taskNameGrouperFactory);

    // Checkpoint config is optional
    if (checkpointConfig != null) {
      initialConfig.putAll(checkpointConfig.build());
    }

    // Adding all systems
    for (SystemConfig systemConfig: systemConfigs) {
      initialConfig.putAll(systemConfig.build());
    }

    Joiner rewritersJoiner = Joiner.on(",").skipNulls();
    String rewriters = null;
    for (RewriterConfig rewriterConfig: rewriterConfigList) {
      initialConfig.putAll(rewriterConfig.build());
      rewriters = rewritersJoiner.join(rewriters, rewriterConfig.name);
    }
    if (rewriterConfigList.size() > 0) {
      initialConfig.put(JOB_CONFIG_REWRITERS, rewriters);
      return Util.rewriteConfig(new MapConfig(initialConfig));
    }

    return new MapConfig(initialConfig);
  }

  private class SystemStreamConfig {
    private static final String STREAM_KEY_SERDE_FORMAT_STRING = "systems.%s.streams.%s.samza.key.serde";
    private static final String STREAM_MSG_SERDE_FORMAT_STRING = "systems.%s.streams.%s.samza.msg.serde";

    public final SystemConfig systemConfig;
    public final String streamName;

    public SerdeConfig keySerde;
    public SerdeConfig msgSerde;

    SystemStreamConfig(SystemConfig systemConfig, String streamName, SerdeConfig keySerde, SerdeConfig msgSerde) {
      if (Strings.isNullOrEmpty(streamName)) {
        throw new SamzaException("Stream Name cannot be null or empty!!");
      }
      this.systemConfig = systemConfig;
      this.streamName = streamName;
      this.keySerde = keySerde;
      this.msgSerde = msgSerde;
    }

    Map<String, String> build() {
      Map<String, String> result = systemConfig.build();
      if (keySerde != null) {
        result.putAll(keySerde.build());
        result.put(String.format(STREAM_KEY_SERDE_FORMAT_STRING, systemConfig.systemName, streamName), keySerde.getName());
      }

      if (msgSerde != null) {
        result.putAll(msgSerde.build());
        result.put(String.format(STREAM_MSG_SERDE_FORMAT_STRING, systemConfig.systemName, streamName), msgSerde.getName());
      }

      return result;
    }
  }
}
