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

package org.apache.samza.container;

import org.apache.samza.config.Config;
import org.apache.samza.context.Context;


/**
 * DO NOT USE! For LinkedIn Only, NOT open source.
 * A life cycle listener to the samza container
 * The interface might change or deprecate in the future.
 */
public interface SamzaContainerLifeCycleListener {

  /**
   * Called before starting any component of a container, including
   * metrics, system producers/consumers, {@link org.apache.samza.task.InitableTask#init(Context)}, etc.
   * @param config config for the job
   */
  void beforeStart(Config config);

  /**
   * Called after all components are started and immediately before running the tasks.
   */
  void afterStart();

  /**
   * Called before shutting down any component of a container.
   */
  void beforeShutdown();

  /**
   * Called after all components are shutdown and immediately before the container finishes shutdown.
   */
  void afterShutdown();
}
