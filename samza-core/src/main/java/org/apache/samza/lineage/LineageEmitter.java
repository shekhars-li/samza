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
package org.apache.samza.lineage;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.LineageConfig;
import org.apache.samza.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The LineageEmitter class helps generate and emit job lineage data to configured sink stream.
 */
public class LineageEmitter {

  private static final Logger LOGGER = LoggerFactory.getLogger(LineageEmitter.class);

  private final List<LineageReporter> reporters;
  private final Config config;

  public LineageEmitter(Config config) {
    this(new LineageConfig(config).getLineageReporterNames().stream().map(name -> {
      String className = new LineageConfig(config).getLineageReporterFactoryClassName(name);
      LineageReporterFactory lineageReporterFactory = ReflectionUtil.getObj(className, LineageReporterFactory.class);
      return lineageReporterFactory.getLineageReporter(config, name);
    }).collect(Collectors.toList()), config);
  }

  @VisibleForTesting
  LineageEmitter(List<LineageReporter> reporters, Config config) {
    this.reporters = reporters;
    this.config = config;
  }

  /**
   * Emit the job lineage information to specified sink stream
   *
   * @param context lineage context
   */
  public void emit(LineageContext context) {
    reporters.forEach(reporter -> {
      try {
        reporter.start();
        reporter.report(context, config);
        reporter.stop();
        LOGGER.info("Emitted lineage data for job '{}' by lineage reporter '{}'",
            new ApplicationConfig(config).getAppName(), reporter);
      } catch (Exception e) {
        // Swallow exception to avoid impacting the application's stability
        // TODO collect exceptions in diagnostic stream or metrics
        LOGGER.error(String.format("Failed to emit lineage data for job '%s' by lineage reporter '%s'",
            new ApplicationConfig(config).getAppName(), reporter), e);
      }
    });
  }
}
