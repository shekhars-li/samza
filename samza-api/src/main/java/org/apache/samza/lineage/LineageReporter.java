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

import org.apache.samza.config.Config;


/**
 * The LineageReporter interface defines how Samza writes generated job lineage information to outside systems, such as
 * messaging systems like Kafka, or file systems.<br/>
 * Implementations are responsible for building lineage data and writing them to their underlying systems.
 */
public interface LineageReporter {

  /**
   * Start the reporter.
   */
  void start();

  /**
   * Stop the reporter.
   */
  void stop();

  /**
   * Parse and generate job lineage data from given lineage context and Samza job config, and send the lineage data to
   * outside system. The lineage data includes job's inputs, outputs and other metadata information.<p/>
   *
   * The config should include full inputs and outputs information, those information are usually populated by job
   * planner and config rewriters. It is the callers' responsibility to make sure config contains all required
   * information before call this function. <p/>
   *
   * The lineage data capture logic may be different depends on given context, e.g. deployment phase vs runtime phase.
   *
   * @param context lineage context
   * @param config Samza job config
   */
  void report(LineageContext context, Config config);
}
