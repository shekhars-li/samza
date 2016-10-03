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
import org.apache.samza.config.MapConfig;
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
 * independent processes <br />
 *
 * <b>Usage Example:</b>
 * <pre>
 * StreamProcessor processor = new StreamProcessor(1, config); <br />
 * processor.start();
 * try {
 *  boolean status = processor.awaitStart(TIMEOUT_MS);    // Optional - blocking call
 *  if (!status) {
 *    // Timed out
 *  }
 *  ...
 * } catch (InterruptedException ie) {
 *   ...
 * } finally {
 *   processor.stop();
 * }
 * </pre>
 *
 */
public class StreamProcessor {
  private static final Logger log = LoggerFactory.getLogger(StreamProcessor.class);
  private static final String JOB_COORDINATOR_FACTORY = "job.coordinator.factory";
  /**
   * processor.id is equivalent to containerId in samza. It is a logical identifier used by Samza for a processor.
   * In a distributed environment, this logical identifier is mapped to a physical identifier of the resource. For
   * example, Yarn provides a "containerId" for every resource it allocates.
   * In an embedded environment, this identifier is provided by the user by directly using the StreamProcessor API.
   *
   * <b>Note:</b>This identifier has to be unique across the instances of StreamProcessors.
   */
  private static final String PROCESSOR_ID = "processor.id";

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
   * SamzaContainer is executed using the ExecutorService. <br />
   *
   * <b>Note:</b> Lifecycle of the ExecutorService is fully managed by the StreamProcessor, and NOT exposed to the user
   *
   * @param processorId Unique identifier for a processor within the same JVM. It has the same semantics as
   *                    "containerId" in Samza
   * @param config Instance of config object - contains all configuration required for processing
   * @param customMetricsReporters Map of custom MetricReporter instances that are to be injected in the Samza job
   */
  public StreamProcessor(int processorId, Config config, Map<String, MetricsReporter> customMetricsReporters) {
    this.executorService = Executors.newSingleThreadExecutor();
    this.processorId = processorId;

    Map<String, String> updatedConfigMap = new HashMap<>();
    updatedConfigMap.putAll(config);
    updatedConfigMap.put(PROCESSOR_ID, String.valueOf(processorId));
    Config updatedConfig = new MapConfig(updatedConfigMap);

    this.jobCoordinator = getJobCoordinatorFactory(updatedConfig).getJobCoordinator(this.processorId, updatedConfig);

    if (new ClusterManagerConfig(updatedConfig).getHostAffinityEnabled()) {
      // Not sure if there is better solution for de-coupling localityManager from the container.
      // Container and JC share the same API for reading/writing locality information
      localityManager = SamzaContainer$.MODULE$.getLocalityManager(this.processorId, updatedConfig);
    } else {
      localityManager = null;
    }

    containerShutdownMs = new TaskConfigJava(updatedConfig).getShutdownMs();
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
   * StreamProcessor Lifecycle: start()
   * <ul>
   * <li>Starts the JobCoordinator and fetches the JobModel</li>
   * <li>Instantiates a SamzaContainer and runs it in the executor</li>
   * </ul>
   * When start() returns, it only guarantees that the container is initialized and submitted to the executor
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
   * Method that allows the user to wait for a specified amount of time for the container to initialize and start
   * processing messages
   *
   * @param timeoutMs Maximum time to wait, in milliseconds
   * @return {@code true}, if the container started within the specified wait time and {@code false} if the waiting time
   *          elapsed
   * @throws InterruptedException if the current thread is interrupted while waiting for container to start-up
   */
  public boolean awaitStart(long timeoutMs) throws InterruptedException {
    return container.awaitStart(timeoutMs);
  }

  /**
   * StreamProcessor Lifecycle: stop()
   * <ul>
   *  <li>Stops the SamzaContainer execution</li>
   *  <li>Stops the JobCoordinator</li>
   *  <li>Shuts down the executorService</li>
   * </ul>
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
      log.error("Ran into problems while trying to stop the container in the processor!", e);
    } catch (TimeoutException e) {
      log.warn("Got Timeout Exception while trying to stop the container in the processor! The processor may not shutdown properly", e);
    }
  }
}
