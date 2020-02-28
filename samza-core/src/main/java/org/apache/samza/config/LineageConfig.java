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
package org.apache.samza.config;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Config helper methods related to Samza job lineage
 */
public class LineageConfig extends MapConfig {

  public static final String LINEAGE_FACTORY = "lineage.factory";
  // reporter name list separated by comma, e.g. reporter1,reporter2
  public static final String LINEAGE_REPORTERS = "lineage.reporters";
  public static final String LINEAGE_REPORTER_FACTORY = "lineage.reporter.%s.factory";

  public LineageConfig(Config config) {
    super(config);
  }

  public Optional<String> getLineageFactoryClassName() {
    return Optional.ofNullable(get(LINEAGE_FACTORY));
  }

  public Set<String> getLineageReporterNames() {
    Optional<String> reporterNames = Optional.ofNullable(get(LINEAGE_REPORTERS));
    if (!reporterNames.isPresent() || reporterNames.get().isEmpty()) {
      return Collections.emptySet();
    }
    return Stream.of(reporterNames.get().split(",")).map(String::trim).collect(Collectors.toSet());
  }

  public Optional<String> getLineageReporterFactoryClassName(String name) {
    return Optional.ofNullable(get(String.format(LINEAGE_REPORTER_FACTORY, name)));
  }
}
