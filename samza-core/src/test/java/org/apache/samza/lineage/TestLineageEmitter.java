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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.LineageConfig;
import org.apache.samza.config.MapConfig;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


public class TestLineageEmitter {

  private final static String MOCK_REPORTER_NAME = "mockReporter";
  private final static LineageContext LINEAGE_CONTEXT = new LineageContext.Builder(LineagePhase.DEPLOYMENT).build();

  @Test(expected = ConfigException.class)
  public void testEmitWhenEnableLineageButNotConfigLineageReporterFactory() {
    new LineageEmitter(new MapConfig(ImmutableMap.of(LineageConfig.LINEAGE_REPORTERS, MOCK_REPORTER_NAME)));
  }

  @Test
  public void testEmit() {
    Config config = new MapConfig(ImmutableMap.of(LineageConfig.LINEAGE_REPORTERS, MOCK_REPORTER_NAME));
    LineageReporter reporter = mock(LineageReporter.class);
    LineageEmitter emitter = new LineageEmitter(Lists.newArrayList(reporter), config);
    emitter.emit(LINEAGE_CONTEXT);
    verify(reporter).start();
    verify(reporter).stop();
    verify(reporter).report(LINEAGE_CONTEXT, config);
  }

}
