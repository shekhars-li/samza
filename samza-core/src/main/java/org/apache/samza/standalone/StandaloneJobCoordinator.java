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

import com.google.common.annotations.VisibleForTesting;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaSystemConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.JobModelManager$;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.apache.samza.util.SystemClock;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import java.util.HashMap;
import java.util.Map;

/**
 * Standalone Job Coordinator does not implement any leader elector module
 * It generates the JobModel using the Config passed into the constructor
 * */
public class StandaloneJobCoordinator implements JobCoordinator {
  private static final Logger log = LoggerFactory.getLogger(StandaloneJobCoordinator.class);
  private static final String LOAD_BALANCED_FORMAT_STRING = "systems.%s.samza.loadBalanced";

  private final int processorId;
  private final Config config;
  private final JobModelManager jobModelManager;

  @VisibleForTesting
  StandaloneJobCoordinator(int processorId, Config config, JobModelManager jobModelManager) {
    this.processorId = processorId;
    this.config = verifyLoadBalancedConsumer(config);
    this.jobModelManager = jobModelManager;
  }

  public StandaloneJobCoordinator(int processorId, Config config) {
    this.processorId = processorId;
    this.config = verifyLoadBalancedConsumer(config);

    JavaSystemConfig systemConfig = new JavaSystemConfig(this.config);
    Map<String, SystemAdmin> systemAdmins = new HashMap<>();
    for (String systemName: systemConfig.getSystemNames()) {
      String systemFactoryClassName = systemConfig.getSystemFactory(systemName);
      if (systemFactoryClassName == null) {
        log.error(String.format("A stream uses system %s, which is missing from the configuration.", systemName));
        throw new SamzaException(String.format("A stream uses system %s, which is missing from the configuration.", systemName));
      }
      SystemFactory systemFactory = Util.getObj(systemFactoryClassName);
      systemAdmins.put(systemName, systemFactory.getAdmin(systemName, this.config));
    }

    StreamMetadataCache streamMetadataCache = new StreamMetadataCache(Util.<String, SystemAdmin>javaMapAsScalaMap(systemAdmins), 5000, SystemClock.instance());

    /** TODO:
     * Locality Manager seems to be required in JC for reading locality info and grouping tasks intelligently and also,
     * in SamzaContainer for writing locality info to the coordinator stream. This closely couples together
     * TaskNameGrouper with the LocalityManager! Hence, groupers should be a property of the jobcoordinator
     * (job.coordinator.task.grouper, instead of task.systemstreampartition.grouper)
     */
    this.jobModelManager = JobModelManager$.MODULE$.getJobCoordinator(config, null, null, streamMetadataCache, null);
  }

  private Config verifyLoadBalancedConsumer(final Config config) {
    TaskConfig taskConfig = new TaskConfig(config);
    Map<String, String> newConfig = new HashMap<>();
    newConfig.putAll(config);

    JavaConversions.setAsJavaSet(taskConfig.getInputStreams()).forEach(ss -> {
      JavaSystemConfig systemConfig = new JavaSystemConfig(config);
      String inputSystem = ss.getSystem();
      if (systemConfig.getSystemFactory(inputSystem) == null) {
        throw new SamzaException(String.format("Factory class config missing for system %s", ss.getStream()));
      }
      SystemFactory factory = Util.getObj(systemConfig.getSystemFactory(inputSystem));
      SystemConsumer consumer = factory.getConsumer(ss.getSystem(), config, new NoOpMetricsRegistry());
      if (!consumer.canSupportLoadBalancedConsumer()) {
        throw new SamzaException(
            String.format(
                "Input system %s does not provide a load-balanced consumer. "
                    + "Cannot run this job in Standalone mode!", inputSystem));
      } else {
        newConfig.put(String.format(LOAD_BALANCED_FORMAT_STRING, inputSystem), "true");
      }
    });

    return new MapConfig(newConfig);
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
