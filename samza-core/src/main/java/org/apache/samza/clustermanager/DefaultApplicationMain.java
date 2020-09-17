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
package org.apache.samza.clustermanager;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.samza.generator.internal.ProcessGeneratorHolder;
import joptsimple.OptionSet;
import org.apache.samza.application.ApplicationUtil;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.ConfigLoader;
import org.apache.samza.config.ConfigLoaderFactory;
import org.apache.samza.config.JobConfig;
import org.apache.samza.runtime.ApplicationRunnerMain;
import org.apache.samza.util.ConfigUtil;
import org.apache.samza.util.ReflectionUtil;


public class DefaultApplicationMain {

  public static void main(String[] args) {
    run(args);
  }

  @VisibleForTesting
  static void run(String[] args) {
    // This branch is ONLY for Yarn deployments, standalone apps uses offspring
    final ApplicationRunnerMain.ApplicationRunnerCommandLine cmdLine = new ApplicationRunnerMain.ApplicationRunnerCommandLine();
    cmdLine.parser().allowsUnrecognizedOptions();

    final OptionSet options = cmdLine.parser().parse(args);
    // start LinkedIn-Specific code.
    // We removed direct calling of loadConfig intended here
    // since `RegExTopicGenerator` calling through loadConfig need to access kafka clients,
    // which can not be accessed now because offSpring is not started.
    // so instead of loading full job config with loadConfig,
    // we execute loadConfig step by step and start offSpring before rewrite configs
    // TODO: will uniform with OSS again after LISAMZA-14802 is done
    final Config submissionConfig = cmdLine.loadConfig(options);
    JobConfig jobConfig = new JobConfig(submissionConfig);

    if (!jobConfig.getConfigLoaderFactory().isPresent()) {
      throw new ConfigException("Missing key " + JobConfig.CONFIG_LOADER_FACTORY + ".");
    }

    ConfigLoaderFactory factory = ReflectionUtil.getObj(jobConfig.getConfigLoaderFactory().get(), ConfigLoaderFactory.class);
    ConfigLoader loader = factory.getLoader(jobConfig.subset(ConfigLoaderFactory.CONFIG_LOADER_PROPERTIES_PREFIX));
    // overrides config loaded with original config, which may contain overridden values.
    Config originalConfig = ConfigUtil.override(loader.getConfig(), submissionConfig);
    /*
     * LinkedIn Only
     *
     * Start the ProcessGenerator with full job config, we don't need to stop and restart it after planning as
     * planning is only expected to change samza related configs but not offspring components.
     */
    ProcessGeneratorHolder.getInstance().createGenerator(originalConfig);
    ProcessGeneratorHolder.getInstance().start();
    Config rewrittenConfig = ConfigUtil.rewriteConfig(originalConfig);
    JobCoordinatorLaunchUtil.run(ApplicationUtil.fromConfig(rewrittenConfig), rewrittenConfig);
    // end Linkedin-Specific code
  }
}
