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
package org.apache.samza.test.harness;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import com.linkedin.samza.context.DefaultLiExternalContextFactory;
import com.linkedin.samza.generator.internal.ProcessGeneratorHolder;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.samza.config.Config;
import org.apache.samza.config.InMemorySystemConfig;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.SystemConfig;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.system.inmemory.InMemorySystemFactory;
import org.apache.samza.test.framework.LiConfigUtil;
import org.junit.After;
import org.junit.Before;


/**
 * Provides helpers for configuring an in-memory system to be used for tests and executing those tests.
 *
 * This is somewhat based on {@link IntegrationTestHarness}, but it avoids using Kafka/Zookeeper.
 */
public class InMemoryIntegrationTestHarness {
  protected static final String IN_MEMORY = "inmemory";

  @Before
  public void setUp() {
    // Linkedin-specific Offspring initialization
    ProcessGeneratorHolder.getInstance().
        createGenerator(new MapConfig(LiConfigUtil.buildRequiredOffspringConfigs(getClass().getSimpleName())));
    ProcessGeneratorHolder.getInstance().start();
  }

  @After
  public void tearDown() {
    // Linkedin-specific Offspring shutdown
    ProcessGeneratorHolder.getInstance().stop();
  }

  protected Config baseInMemorySystemConfigs() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(String.format(SystemConfig.SYSTEM_FACTORY_FORMAT, IN_MEMORY), InMemorySystemFactory.class.getName());
    configMap.put(InMemorySystemConfig.INMEMORY_SCOPE, RandomStringUtils.random(10, true, true));
    configMap.put(JobConfig.JOB_DEFAULT_SYSTEM, IN_MEMORY);
    return new MapConfig(configMap);
  }

  protected void executeRun(ApplicationRunner applicationRunner, Config config) {
    applicationRunner.run(buildExternalContext(config).orElse(null));
  }

  private Optional<ExternalContext> buildExternalContext(Config config) {
    // Linkedin-specific external context generation
    return Optional.of(
        ProcessGeneratorHolder.getInstance().getGenerator().getBean(DefaultLiExternalContextFactory.class));
  }
}
