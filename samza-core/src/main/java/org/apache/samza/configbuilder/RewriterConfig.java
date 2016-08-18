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

import java.util.Collections;
import java.util.Map;

/**
 * TODO: Deprecate config rewriters in Samza altogether
 * The main reason for introducing was to dynamically pick-up topic names based on the regex. This is only partially useful.
 * We should continuously poll the source for new topics and re-balance the partition in the job. Similar to the idea
 * proposed in SAMZA-237
 */
public abstract class RewriterConfig {
  protected final String name;
  private final String klassName;

  public RewriterConfig(String name, String klassName) {
    this.name = name;
    this.klassName = klassName;
  }

  Map<String, String> build() {
    if (name == null | klassName == null) {
      throw new ConfigException("Rewriter name and rewriter class name cannot be null!");
    }
    return Collections.singletonMap(String.format("job.config.rewriter.%s.class", name), klassName);
  }
}
