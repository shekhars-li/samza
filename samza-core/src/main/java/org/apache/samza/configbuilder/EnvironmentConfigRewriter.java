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

/**
 * EnvironmentConfigRewriter is a builder class for using {@link org.apache.samza.config.EnvironmentConfigRewriter}. It
 * iterates through all System Environment variables and adds it as a Samza config. See
 * {@link org.apache.samza.config.EnvironmentConfigRewriter} for more details.
 */
public class EnvironmentConfigRewriter extends RewriterConfig {
  private static final String REWRITER_CLASS_NAME = "org.apache.samza.config.EnvironmentConfigRewriter";

  private final Map<String, String> envMap = new HashMap<>();

  public EnvironmentConfigRewriter(String name, Map<String, String> envMap) {
    super(name, REWRITER_CLASS_NAME);
    if (envMap != null) {
      this.envMap.putAll(envMap);
    }
  }

  public EnvironmentConfigRewriter(String name) {
    this(name, System.getenv());
  }
}
