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

import junit.framework.Assert;
import org.apache.samza.config.Config;
import org.apache.samza.container.grouper.stream.AllSspToSingleTaskGrouperFactory;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestStandaloneConfigBuilder {
  ConfigBuilder builder;

  @Before
  public void setup() {
    KafkaSystemConfig kafkaSystem = new KafkaSystemConfig("kafka-system", "127.0.0.1:2181", "127.0.0.1:9092");
    SerdeConfig stringSerde = new SerdeConfig("string", SerdeConfig.SerdeAlias.STRING);

    builder = ConfigBuilder.getStandaloneConfigBuilder("test-job", "org.apache.samza.configbuilder.MyStreamTask")
        .addTaskInput(kafkaSystem, "numbers", stringSerde, stringSerde)
        .setCheckpointConfig(new KafkaCheckpointConfig(kafkaSystem));
  }

  @After
  public void teardown() {
    builder = null;
  }

  @Test
  public void testGroupersAreCorrect() {
    Config config = builder.build();

    Assert.assertEquals(
        AllSspToSingleTaskGrouperFactory.class.getName(),
        config.get(ConfigBuilder.SSP_GROUPER_FACTORY));

    Assert.assertEquals(
        SingleContainerGrouperFactory.class.getName(),
        config.get(ConfigBuilder.TASK_NAME_GROUPER_FACTORY));
  }
}
