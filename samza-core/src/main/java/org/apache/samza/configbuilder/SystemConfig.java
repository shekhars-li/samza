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

public abstract class SystemConfig {
  private static final String SYSTEM_FACTORY_REGEX = "systems.%s.samza.factory";
  private static final String SYSTEM_KEY_SERDE_REGEX = "systems.%s.samza.key.serde";
  private static final String SYSTEM_MSG_SERDE_REGEX = "systems.%s.samza.msg.serde";

  // Mandatory
  public final String systemName;
  public final String systemFactory;

  // Optional
  public SerdeConfig keySerde;
  public SerdeConfig msgSerde;

  public SystemConfig(String systemName, String systemFactory) {
    this (systemName, systemFactory, null, null);
  }

  /**
   * SystemConfig describes the system name, factory class and the serde configurations for key and message
   * @param systemName Name of the system
   * @param systemFactory Class Name of the System's Factory Class
   * @param keySerde (Nullable) Serde configuration for Keys at System-level
   *                 If null, then serde step is simply a pass-through
   * @param msgSerde (Nullable) Serde configuration for Messages at System-level
   *                 If null, then serde step is simply a pass-through
   */
  public SystemConfig(String systemName, String systemFactory, SerdeConfig keySerde, SerdeConfig msgSerde) {
    this.systemName = systemName;
    this.systemFactory = systemFactory;
    this.keySerde = keySerde;
    this.msgSerde = msgSerde;
  }

  public Map<String, String> build() {
    Map<String, String> result = new HashMap<>();
    result.put(String.format(SYSTEM_FACTORY_REGEX, systemName), systemFactory);

    if (keySerde != null) {
      result.putAll(keySerde.build());
      result.put(String.format(SYSTEM_KEY_SERDE_REGEX, systemName), keySerde.getName());
    }
    if (msgSerde != null) {
      result.putAll(msgSerde.build());
      result.put(String.format(SYSTEM_MSG_SERDE_REGEX, systemName), msgSerde.getName());
    }

    return result;
  }
}
