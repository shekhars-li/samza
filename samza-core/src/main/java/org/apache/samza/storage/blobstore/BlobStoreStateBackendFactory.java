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

package org.apache.samza.storage.blobstore;

import com.google.common.base.Preconditions;

import java.io.File;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.config.Config;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.storage.KafkaChangelogRestoreParams;
import org.apache.samza.storage.StateBackendFactory;
import org.apache.samza.storage.StorageManagerUtil;
import org.apache.samza.storage.TaskBackupManager;
import org.apache.samza.storage.TaskRestoreManager;
import org.apache.samza.storage.TaskStorageAdmin;
import org.apache.samza.storage.blobstore.util.BlobStoreUtil;
import org.apache.samza.util.Clock;
import org.apache.samza.util.ReflectionUtil;


public class BlobStoreStateBackendFactory implements StateBackendFactory {
  @Override
  public TaskBackupManager getBackupManager(
      JobModel jobModel,
      ContainerModel containerModel,
      TaskModel taskModel,
      ExecutorService backupExecutor,
      MetricsRegistry metricsRegistry,
      Config config,
      Clock clock,
      File loggedStoreBaseDir,
      File nonLoggedStoreBaseDir) {
    StorageConfig storageConfig = new StorageConfig(config);
    String blobStoreManagerFactory = storageConfig.getBlobStoreManagerFactory();
    Preconditions.checkState(StringUtils.isNotBlank(blobStoreManagerFactory));
    BlobStoreManagerFactory factory = ReflectionUtil.getObj(blobStoreManagerFactory, BlobStoreManagerFactory.class);
    BlobStoreManager backupBlobStoreManager = factory.getBackupBlobStoreManager(config, backupExecutor);
    BlobStoreUtil blobStoreUtil = new BlobStoreUtil(backupBlobStoreManager, backupExecutor);
    return new BlobStoreTaskStorageBackupManager(jobModel, containerModel, taskModel, backupExecutor, config, clock,
        loggedStoreBaseDir, new StorageManagerUtil(), blobStoreUtil);
  }

  @Override
  public TaskRestoreManager getRestoreManager(
      JobContext jobContext,
      ContainerContext containerContext,
      TaskModel taskModel,
      ExecutorService restoreExecutor,
      MetricsRegistry metricsRegistry,
      Config config,
      Clock clock,
      File loggedStoreBaseDir,
      File nonLoggedStoreBaseDir,
      KafkaChangelogRestoreParams kafkaChangelogRestoreParams) {
    StorageConfig storageConfig = new StorageConfig(config);
    String blobStoreManagerFactory = storageConfig.getBlobStoreManagerFactory();
    Preconditions.checkState(StringUtils.isNotBlank(blobStoreManagerFactory));
    BlobStoreManagerFactory factory = ReflectionUtil.getObj(blobStoreManagerFactory, BlobStoreManagerFactory.class);
    BlobStoreManager blobStoreManager = factory.getRestoreBlobStoreManager(config, restoreExecutor);
    BlobStoreUtil blobStoreUtil = new BlobStoreUtil(blobStoreManager, restoreExecutor);
    return new BlobStoreTaskStorageRestoreManager(taskModel, restoreExecutor, config, new StorageManagerUtil(),
        blobStoreUtil, loggedStoreBaseDir, nonLoggedStoreBaseDir);
  }

  @Override
  public TaskStorageAdmin getAdmin() {
    // TODO BLOCKER shesharm fix this
    return null;
  }
}