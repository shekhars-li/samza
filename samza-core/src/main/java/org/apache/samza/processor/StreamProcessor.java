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
package org.apache.samza.processor;

import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.container.LocalityManager;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.SamzaContainer$;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorFactory;
import org.apache.samza.metrics.JmxServer;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * StreamProcessor can be embedded in any application or executed in a distributed environment (aka cluster) as
 * independent processes
 *
 * Usage Example:
 * ConfigBuilder builder = ConfigBuilder
 *                          .getStandaloneBuilder("job-name", "stream-task-class-name")
 *                          .addTaskInput ...
 * ...
 * Config config = builder.build();
 * StreamProcessor processor = new StreamProcessor(1, config);
 * processor.start();
 *
 * ...
 *
 * processor.stop();
 *
 */
public class StreamProcessor {
  private static final Logger log = LoggerFactory.getLogger(StreamProcessor.class);
  private static final String JOB_COORDINATOR_FACTORY = "job.coordinator.factory";

  private Map<String, MetricsReporter> customMetricsReporters = new HashMap<>();
  private final int processorId;
  private final ExecutorService executorService;
  private Future containerFuture;
  private final LocalityManager localityManager;
  private final JobCoordinator jobCoordinator;
  private final long containerShutdownMs;

  private volatile SamzaContainer container;
  /**
   * Create an instance of StreamProcessor that encapsulates a JobCoordinator and Samza Container
   *
   * JobCoordinator controls how the various StreamProcessor instances belonging to a job coordinate. It is also
   * responsible generating and updating JobModel.
   * When StreamProcessor starts, it starts the JobCoordinator and brings up a SamzaContainer based on the JobModel.
   * SamzaContainer is executed using the ExecutorService.
   *
   * Note: Lifecycle of the ExecutorService is fully managed by the StreamProcessor, and NOT exposed to the user
   *
   * @param processorId Unique identifier for a processor within the same JVM. It has the same semantics as
   *                    "containerId" in Samza
   * @param config Instance of config object - contains all configuration required for processing
   * @param customMetricsReporters Map of custom MetricReporter instances that are to be injected in the Samza job
   */
  public StreamProcessor(int processorId, Config config, Map<String, MetricsReporter> customMetricsReporters) {
    this.executorService = Executors.newSingleThreadExecutor();
    this.processorId = processorId;
    this.jobCoordinator = getJobCoordinatorFactory(config).getJobCoordinator(this.processorId, config);

    if (new ClusterManagerConfig(config).getHostAffinityEnabled()) {
      // Not sure if there is better solution for de-coupling localityManager from the container.
      // Container and JC share the same API for reading/writing locality information
      localityManager = SamzaContainer$.MODULE$.getLocalityManager(this.processorId, config);
    } else {
      localityManager = null;
    }

    containerShutdownMs = new TaskConfigJava(config).getShutdownMs();
    if (customMetricsReporters != null) {
      this.customMetricsReporters.putAll(customMetricsReporters);
    }
  }


  private JobCoordinatorFactory getJobCoordinatorFactory(Config config) {
    if (config.get(JOB_COORDINATOR_FACTORY) == null) {
      throw new ConfigException(
          String.format("Missing config - %s. Cannot start StreamProcessor!", JOB_COORDINATOR_FACTORY));
    }
    return Util.<JobCoordinatorFactory>getObj(config.get("job.coordinator.factory"));
  }
  /**
   * StreamProcessor Lifecycle: start
   * - Starts the JobCoordinator and fetches the JobModel
   * - Instantiates a SamzaContainer and runs it in the executor
   */
  public void start() {
    jobCoordinator.start();

    container = SamzaContainer$.MODULE$.apply(
        this.processorId,
        jobCoordinator.getJobModel(),
        localityManager,
        new JmxServer(),
        Util.<String, MetricsReporter>javaMapAsScalaMap(customMetricsReporters));

    runContainer();
  }

  /**
   * StreamProcessor Lifecycle: stop
   * - Stops the SamzaContainer execution
   * - Stops the JobCoordinator
   * - Shuts down the executorService
   */
  public void stop() {
    stopContainer();
    jobCoordinator.stop();
    executorService.shutdown();
  }

  private void runContainer() {
    containerFuture = executorService.submit(new Runnable() {
      @Override
      public void run() {
        container.run();
      }
    });
  }

  private void stopContainer() {
    container.shutdown();
    try {
      containerFuture.get(containerShutdownMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException e) {
      log.error("Ran into problems while trying to stop the processor!", e);
    } catch (TimeoutException e) {
      log.warn("Got Timeout Exception while trying to stop the processor! The processor may not have shutdown completely!", e);
    }
  }
}
