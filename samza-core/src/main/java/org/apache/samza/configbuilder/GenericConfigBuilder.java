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
import org.apache.samza.config.Config;

public class GenericConfigBuilder extends ConfigBuilder {

  public GenericConfigBuilder(
      String jobName,
      String taskClass) {
    super(jobName, taskClass);
  }

  public GenericConfigBuilder setSspGrouper(String sspGrouperFactory) {
    this.sspGrouperFactory = sspGrouperFactory;
    return this;
  }

  public GenericConfigBuilder setTaskNameGrouper(String taskNameGrouperFactory) {
    this.taskNameGrouperFactory = taskNameGrouperFactory;
    return this;
  }

  public GenericConfigBuilder setJobCoordinatorFactory(String jobCoordinatorFactory) {
    this.jobCoordinatorFactory = jobCoordinatorFactory;
    return this;
  }

  @Override
  public Config build() {
    if (jobCoordinatorFactory == null || jobCoordinatorFactory.isEmpty()) {
      throw new SamzaException("jobCoordinator not specified!");
    }

    if (sspGrouperFactory == null || sspGrouperFactory.isEmpty()) {
      throw new SamzaException("sspGrouper not specified!");
    }

    if (taskNameGrouperFactory == null || taskNameGrouperFactory.isEmpty()) {
      throw new SamzaException("taskNameGrouper not specified!");
    }
    return super.build();
  }
}

