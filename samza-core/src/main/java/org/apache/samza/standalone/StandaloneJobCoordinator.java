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
package org.apache.samza.standalone;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaSystemConfig;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.JobModelManager$;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.util.SystemClock;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Standalone Job Coordinator does not implement any leader elector module
 * It generates the JobModel using the Config passed into the constructor
 * */
public class StandaloneJobCoordinator implements JobCoordinator {
  private static final Logger log = LoggerFactory.getLogger(StandaloneJobCoordinator.class);

  private final int processorId;
  private final Config config;
  private final JobModelManager jobModelManager;

  public StandaloneJobCoordinator(int processorId, Config config) {
    this.processorId = processorId;
    this.config = config;
    JavaSystemConfig systemConfig = new JavaSystemConfig(this.config);
    Map<String, SystemAdmin> systemAdmins = new HashMap<>();
    for (String systemName: systemConfig.getSystemNames()) {
      String systemFactoryClassName = systemConfig.getSystemFactory(systemName);
      if (systemFactoryClassName == null) {
        log.error(String.format("A stream uses system %s, which is missing from the configuration.", systemName));
        throw new SamzaException(String.format("A stream uses system %s, which is missing from the configuration.", systemName));
      }
      SystemFactory systemFactory = (SystemFactory) Util.getObj(systemFactoryClassName);
      systemAdmins.put(systemName, systemFactory.getAdmin(systemName, this.config));
    }

    StreamMetadataCache streamMetadataCache = new StreamMetadataCache(Util.javaMapAsScalaMap(systemAdmins), 5000, SystemClock.instance());

    /** TODO:
     * Locality Manager seems to be required in JC for reading locality info and grouping tasks intelligently and also,
     * in SamzaContainer for writing locality info to the coordinator stream. This closely couples together
     * TaskNameGrouper with the LocalityManager! Hence, groupers should be a property of the jobcoordinator
     * (job.coordinator.task.grouper, instead of task.systemstreampartition.grouper)
     */
    this.jobModelManager = JobModelManager$.MODULE$.getJobCoordinator(config, null, null, streamMetadataCache, null);
  }

  @Override
  public void start() {
    // No-op
  }

  @Override
  public void stop() {
    // No-op
  }

  @Override
  public int getProcessorId() {
    return this.processorId;
  }

  @Override
  public JobModel getJobModel() {
    return jobModelManager.jobModel();
  }
}