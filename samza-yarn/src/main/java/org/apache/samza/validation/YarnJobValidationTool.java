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

package org.apache.samza.validation;

import joptsimple.OptionSet;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.job.yarn.ClientHelper;
import org.apache.samza.util.hadoop.HttpFileSystem;
import org.apache.samza.util.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command-line tool for validating the status of a Yarn job.
 * It checks the job has been successfully submitted to the Yarn cluster, the status of
 * the application attempt is running and the container count matches the expectation.
 * This tool can be used, for example, as a quick validation step after starting a job.
 *
 * When running this tool, please provide the configuration URI of job. For example:
 *
 * deploy/samza/bin/validate-yarn-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-feed.properties
 *
 * The tool prints out the validation result in each step and throws an exception when the
 * validation fails.
 */
public class YarnJobValidationTool {
  private static final Logger log = LoggerFactory.getLogger(YarnJobValidationTool.class);

  private final JobConfig config;
  private final YarnClient client;
  private final String jobName;

  public YarnJobValidationTool(JobConfig config, YarnClient client) {
    this.config = config;
    this.client = client;
    String name = this.config.getName().get();
    String jobId = this.config.getJobId().nonEmpty()? this.config.getJobId().get() : "1";
    this.jobName =  name + "_" + jobId;
  }

  public void run() {
    ApplicationId appId;
    ApplicationAttemptId attemptId;

    try {
      log.info("Start validating job " + this.jobName);

      appId = validateAppId();
      attemptId = validateRunningAttemptId(appId);
      validateContainerCount(attemptId);

      log.info("End validation");
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      System.exit(1);
    }
  }

  public ApplicationId validateAppId() throws Exception {
    ApplicationId appId = null;
    for(ApplicationReport applicationReport : this.client.getApplications()) {
      if(applicationReport.getName().equals(this.jobName)) {
        appId = applicationReport.getApplicationId();
        break;
      }
    }
    if (appId != null) {
      log.info("Job submit success. ApplicationId " + appId.getId());
      return appId;
    } else {
      throw new SamzaException("Job submit failure " + this.jobName);
    }
  }

  public ApplicationAttemptId validateRunningAttemptId(ApplicationId appId) throws Exception {
    ApplicationAttemptId attemptId = null;
    for(ApplicationAttemptReport attemptReport : this.client.getApplicationAttempts(appId)) {
      if (attemptReport.getYarnApplicationAttemptState() == YarnApplicationAttemptState.RUNNING) {
        attemptId = attemptReport.getApplicationAttemptId();
        break;
      }
    }
    if (attemptId != null) {
      log.info("Job is running. AttempId " + attemptId.getAttemptId());
      return attemptId;
    } else {
      throw new SamzaException("Job not running " + this.jobName);
    }
  }

  public int validateContainerCount(ApplicationAttemptId attemptId) throws Exception {
    int containerCount = this.client.getContainers(attemptId).size();
    // expected containers to be the configured job containers plus the AppMaster container
    int containerExpected = this.config.getContainerCount() + 1;

    if (containerCount == containerExpected) {
      log.info("Container count matches. " + containerCount + " containers are running.");
      return containerCount;
    } else {
      throw new SamzaException("Container count does not match. " + containerCount + " containers are running, while " + containerExpected + " is expected.");
    }
  }

  public static void main(String [] args) {
    CommandLine cmdline = new CommandLine();
    OptionSet options = cmdline.parser().parse(args);
    Config config = cmdline.loadConfig(options);
    YarnConfiguration hadoopConfig = new YarnConfiguration();
    hadoopConfig.set("fs.http.impl", HttpFileSystem.class.getName());
    hadoopConfig.set("fs.https.impl", HttpFileSystem.class.getName());
    ClientHelper clientHelper = new ClientHelper(hadoopConfig);

    new YarnJobValidationTool(new JobConfig(config), clientHelper.yarnClient()).run();
  }
}
