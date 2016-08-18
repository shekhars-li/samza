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

import java.util.Collections;
import java.util.Map;

public class SerdeConfig {
  private static final String SERDE_FACTORY_CLASS_FORMAT = "org.apache.samza.serializers.%s";

  public enum SerdeAlias {
    BYTE, INT, STRING, BYTE_BUFFER, JSON, DOUBLE
  }

  private final String name;
  private final String klass;

  public SerdeConfig(String name, String klass) {
    this.name = name;
    this.klass = klass;
  }

  public SerdeConfig(String name, SerdeAlias serdeAlias) {
    this.name = name;
    switch (serdeAlias) {
      case INT:
        this.klass = String.format(SERDE_FACTORY_CLASS_FORMAT, "IntegerSerdeFactory");
        break;
      case BYTE:
        this.klass = String.format(SERDE_FACTORY_CLASS_FORMAT, "ByteSerdeFactory");
        break;
      case STRING:
        this.klass = String.format(SERDE_FACTORY_CLASS_FORMAT, "StringSerdeFactory");
        break;
      case BYTE_BUFFER:
        this.klass = String.format(SERDE_FACTORY_CLASS_FORMAT, "ByteBufferSerdeFactory");
        break;
      case JSON:
        this.klass = String.format(SERDE_FACTORY_CLASS_FORMAT, "JsonSerdeFactory");
        break;
      case DOUBLE:
        this.klass = String.format(SERDE_FACTORY_CLASS_FORMAT, "DoubleSerdeFactory");
        break;
      default:
        // Instead of throwing exception, do we assumed byte??
        throw new SamzaException("Unknown Serde Definition! "); // TODO: Define a custom Config Exception for Samza
    }
  }

  public Map<String, String> build() {
    return Collections.singletonMap(String.format("serializers.registry.%s.class", name), klass);
  }

  public String getName() {
    return name;
  }
}
