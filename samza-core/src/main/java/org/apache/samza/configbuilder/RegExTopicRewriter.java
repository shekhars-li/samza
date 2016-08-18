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

import org.apache.samza.config.ConfigException;

import java.util.HashMap;
import java.util.Map;

public class RegExTopicRewriter extends RewriterConfig {
  private static final String REWRITER_CLASS_NAME = "org.apache.samza.config.RegExTopicGenerator";
  private static final String REGEX_SYSTEM_FORMAT_STRING = "job.config.rewriter.%s.system";
  private static final String REGEX_STREAM_FORMAT_STRING = "job.config.rewriter.%s.regex";
  private static final String STREAM_CONFIG_FORMAT_STRING = "job.config.rewriter.%s.config.%s";

  final String regexStreamName;
  final KafkaSystemConfig kafkaSystemConfig;
  final Map<String, String> regexStreamProperties = new HashMap<>();

  public RegExTopicRewriter(String name, String regexStreamName, KafkaSystemConfig kafkaSystemConfig, Map<String, String> regexStreamProperties) {
    super(name, REWRITER_CLASS_NAME);
    this.regexStreamName = regexStreamName;
    this.kafkaSystemConfig = kafkaSystemConfig;
    if (regexStreamProperties != null) {
      this.regexStreamProperties.putAll(regexStreamProperties);
    }
  }

  public RegExTopicRewriter(String name, String regexStreamName, KafkaSystemConfig kafkaSystemConfig) {
    this(name, regexStreamName, kafkaSystemConfig, new HashMap<String, String>());
  }

  @Override
  public Map<String, String> build() {
    if (kafkaSystemConfig == null) {
      throw new ConfigException("KafkaSystemConfig cannot be empty for RegExTopicRewriter!");
    }

    Map<String, String> config = super.build();
    config.putAll(kafkaSystemConfig.build());
    config.put(String.format(REGEX_SYSTEM_FORMAT_STRING, name), kafkaSystemConfig.systemName);
    config.put(String.format(REGEX_STREAM_FORMAT_STRING, name), regexStreamName);
    for (Map.Entry<String, String> entry: regexStreamProperties.entrySet()) {
      config.put(String.format(STREAM_CONFIG_FORMAT_STRING, name, entry.getKey()), entry.getValue());
    }

    return config;
  }
}
