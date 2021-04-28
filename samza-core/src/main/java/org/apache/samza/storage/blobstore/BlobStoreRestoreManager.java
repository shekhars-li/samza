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

import com.google.common.collect.ImmutableSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.samza.container.TaskName;
import org.apache.samza.storage.blobstore.index.DirIndex;
import org.apache.samza.storage.blobstore.index.SnapshotIndex;
import org.apache.samza.storage.blobstore.metrics.BlobStoreRestoreManagerMetrics;
import org.apache.samza.storage.blobstore.util.BlobStoreStateBackendUtil;
import org.apache.samza.storage.blobstore.util.BlobStoreUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.config.Config;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.storage.StorageManagerUtil;
import org.apache.samza.storage.TaskRestoreManager;
import org.apache.samza.util.FutureUtil;
import org.apache.samza.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BlobStoreRestoreManager implements TaskRestoreManager {
  private static final Logger LOG = LoggerFactory.getLogger(BlobStoreRestoreManager.class);

  private final TaskModel taskModel;
  private final ExecutorService executor;
  private final Config config;
  private final StorageManagerUtil storageManagerUtil;
  private final BlobStoreUtil blobStoreUtil;
  private final File loggedBaseDir;
  private final File nonLoggedBaseDir;
  private final String taskName;

  private final BlobStoreRestoreManagerMetrics metrics;

  /**
   * Map of store name and Pair of blob id of SnapshotIndex and the corresponding SnapshotIndex from last snapshot
   * creation
   */
  private Map<String, Pair<String, SnapshotIndex>> prevStoreSnapshotIndexes;

  public BlobStoreRestoreManager(TaskModel taskModel, ExecutorService restoreExecutor,
      BlobStoreRestoreManagerMetrics metrics, Config config, StorageManagerUtil storageManagerUtil,
      BlobStoreUtil blobStoreUtil, File loggedBaseDir, File nonLoggedBaseDir) {
    this.taskModel = taskModel;
    this.executor = restoreExecutor; // TODO BLOCKER pmaheshw dont block on restore executor
    this.config = config;
    this.storageManagerUtil = storageManagerUtil;
    this.blobStoreUtil = blobStoreUtil;
    this.prevStoreSnapshotIndexes = new HashMap<>();
    this.loggedBaseDir = loggedBaseDir;
    this.nonLoggedBaseDir = nonLoggedBaseDir;
    this.taskName = taskModel.getTaskName().getTaskName();
    this.metrics = metrics;
  }

  @Override
  public void init(Checkpoint checkpoint) {
    long startTime = System.nanoTime();
    // get previous SCMs from checkpoint
    LOG.debug("Initializing blob store restore manager for task: {}", taskName);
    prevStoreSnapshotIndexes = BlobStoreStateBackendUtil.getStoreSnapshotIndexes(taskName, checkpoint, blobStoreUtil);
    metrics.getSnapshotIndexNs.set(System.nanoTime() - startTime);
    LOG.trace("Found previous snapshot index during blob store restore manager init for task: {} to be: {}",
        taskName, prevStoreSnapshotIndexes);

    metrics.initStoreMetrics(prevStoreSnapshotIndexes.keySet());

    // TODO BLOCKER shesharm move to task store admin.
    // TODO BLOCKER shesharm only use stores managed by this manager (check backend factory)
    List<String> currentTaskStores = new StorageConfig(config).getStoreNames();
    // Note: blocks the caller (main) thread.
    deleteUnusedStoresFromBlobStore(taskName, currentTaskStores, prevStoreSnapshotIndexes, blobStoreUtil, executor);
    metrics.initNs.set(System.nanoTime() - startTime);
  }

  /**
   * Restore state from checkpoints, state snapshots and changelog.
   */
  @Override
  public void restore() {
    LOG.debug("Starting restore for task: {} stores: {}", taskName, prevStoreSnapshotIndexes.keySet());

    long restoreStartTime = System.nanoTime();
    AtomicInteger filesToRestore = new AtomicInteger(0);
    AtomicLong bytesToRestore = new AtomicLong(0);
    AtomicInteger filesRestored = new AtomicInteger(0);
    AtomicLong bytesRestored = new AtomicLong(0L);

    List<CompletionStage<Void>> restoreFutures = new ArrayList<>();
    prevStoreSnapshotIndexes.forEach((storeName, scmAndSnapshotIndex) -> {
      long storeRestoreStartTime = System.nanoTime();
      SnapshotIndex snapshotIndex = scmAndSnapshotIndex.getRight();
      DirIndex dirIndex = snapshotIndex.getDirIndex();
      filesToRestore.addAndGet(dirIndex.getFilesPresent().size());
      bytesToRestore.addAndGet(dirIndex.getFilesPresent().stream().mapToLong(fi -> fi.getFileMetadata().getSize()).sum());

      CheckpointId checkpointId = snapshotIndex.getSnapshotMetadata().getCheckpointId();

      File storeDir = storageManagerUtil.getTaskStoreDir(loggedBaseDir, storeName,
          taskModel.getTaskName(), TaskMode.Active);
      String storeCheckpointDir = StorageManagerUtil.getCheckpointDirPath(storeDir, checkpointId);
      Path storeCheckpointDirPath = Paths.get(storeCheckpointDir);
      LOG.trace("Got task: {} store: {} local store directory: {} and local store checkpoint directory: {}",
          taskName, storeName, storeDir, storeCheckpointDir);

      // we always delete the store dir to preserve transactional state guarantees.
      try {
        LOG.debug("Deleting local store directory: {}. Will be restored from local store checkpoint directory " +
            "or remote snapshot.", storeDir);
        FileUtils.deleteDirectory(storeDir);
      } catch (IOException e) {
        throw new SamzaException(String.format("Error deleting store directory: %s", storeDir), e);
      }

      Set<String> filesToIgnore = ImmutableSet.of(
          StorageManagerUtil.OFFSET_FILE_NAME_LEGACY,
          StorageManagerUtil.OFFSET_FILE_NAME_NEW,
          StorageManagerUtil.SIDE_INPUT_OFFSET_FILE_NAME_LEGACY,
          StorageManagerUtil.CHECKPOINT_FILE_NAME);

      // TODO BLOCKER pmaheshw: add error handling e.g. on sameDir comparision failures.
      // if a store checkpoint directory exists for the last successful task checkpoint, try to use it.
      boolean restoreStore;
      if (Files.exists(storeCheckpointDirPath)) {
        if (new StorageConfig(config).getCleanLoggedStoreDirsOnStart(storeName)) {
          LOG.debug("Restoring task: {} store: {} from remote snapshot since the store is configured to be " +
              "restored on each restart.", taskName, storeName);
          restoreStore = true;
        } else if (blobStoreUtil.areSameDir(filesToIgnore, true).test(storeCheckpointDirPath.toFile(), dirIndex)) {
        // TODO BLOCKER shesharm check if the checkpoint directory contents are valid (i.e. identical to remote snapshot)
        // exclude the "OFFSET" family of files files etc that are written to the checkpoint dir
        // after the remote upload is complete as part of TaskStorageCommitManager#writeCheckpointToStoreDirectories.
        // TODO HIGH shesharm add tests that the exclude works correctly and that it doesn't always restore fully

          LOG.debug("Renaming store checkpoint directory: {} to store directory: {} since its contents are identical " +
              "to the remote snapshot.", storeCheckpointDirPath, storeDir);
          // atomically rename the checkpoint dir to the store dir
          new FileUtil().move(storeCheckpointDirPath.toFile(), storeDir);
          restoreStore = false; // no restore required for this store.
        } else {
          // we don't optimize for the case when the local host doesn't contain the most recent store checkpoint
          // directory but contains an older checkpoint directory which could have partial overlap with the remote
          // snapshot. we also don't try to optimize for any edge cases where the most recent checkpoint directory
          // contents could be partially different than the remote store (afaik, there is no known valid scenario
          // where this could happen right now, except for the offset file handling above).
          // it's simpler and fast enough for now to restore the entire store instead.

          LOG.error("Local store checkpoint directory: {} contents are not the same as the remote snapshot. " +
              "Queuing for restore from remote snapshot.", storeCheckpointDir);
          // old checkpoint directory will be deleted later during commits
          restoreStore = true;
        }
      } else { // did not find last checkpoint dir, restore the store from the remote blob store
        LOG.debug("No local store checkpoint directory found at: {}. " +
            "Queuing for restore from remote snapshot.", storeCheckpointDir);
        restoreStore = true;
      }

      if (restoreStore) { // restore the store from the remote blob store
        LOG.debug("Deleting local store checkpoint directory: {} before restore.", storeCheckpointDirPath);
        // delet all store chckpoint directories. if we only delete the store directory and don't
        // delete the checkpoint directories, the store size on disk will grow to 2x after restore
        // until the first commit is completed and older checkpoint dirs are deleted. This is
        // because the hard-linked checkpoint dir files will no longer be de-duped with the
        // now-deleted main store directory contents and will take up additional space of their
        // own during the restore.
        try {
          List<File> checkpointDirs = storageManagerUtil.getTaskStoreCheckpointDirs(
              loggedBaseDir, storeName, new TaskName(taskName), TaskMode.Active);
          for (File checkpointDir: checkpointDirs) {
            FileUtils.deleteDirectory(checkpointDir);
          }
        } catch (Exception e) {
          throw new SamzaException(
              String.format("Error deleting local store checkpoint directory: %s before restore.",
                  storeCheckpointDirPath));
        }

        CompletableFuture<Void> restoreFuture =
            FutureUtil.allOf(blobStoreUtil.restoreDir(storeDir, dirIndex))
                .thenRunAsync(() -> {
                  long restoreTimeNs = System.nanoTime() - storeRestoreStartTime;
                  metrics.storeRestoreNs.get(storeName).set(restoreTimeNs);

                  long dirIndexBytesRestored =
                      dirIndex.getFilesPresent().stream().mapToLong(fi -> fi.getFileMetadata().getSize()).sum();
                  metrics.filesYetToRestore.set(filesToRestore.longValue() - dirIndex.getFilesPresent().size());
                  metrics.bytesYetToRestore.set(bytesToRestore.longValue() - dirIndexBytesRestored);

                  filesRestored.addAndGet(dirIndex.getFilesPresent().size());
                  bytesRestored.addAndGet(dirIndexBytesRestored);

                  long downloadMbps = (dirIndexBytesRestored / restoreTimeNs) * 8000;
                  metrics.downloadMbps.set(downloadMbps);

                  LOG.trace("Comparing restored store directory: {} and remote directory to verify restore.", storeDir);
                  if (!blobStoreUtil.areSameDir(filesToIgnore, false).test(storeDir, dirIndex)) {
                    throw new SamzaException(String.format("Restored store directory: %s contents " +
                        "are not the same as the remote snapshot.", storeDir.getAbsolutePath()));
                  } else {
                    LOG.info("Restore from remote snapshot completed for store: {}", storeDir);
                  }
                }, executor);
        restoreFutures.add(restoreFuture);
      }
    });

    // wait for all restores to finish
    FutureUtil.allOf(restoreFutures).join(); // TODO BLOCKER pmaheshw remove
    LOG.info("Restore completed for task: {} stores: {}", taskName, prevStoreSnapshotIndexes.keySet());
    // update metrics
    metrics.restoreNs.set(System.nanoTime() - restoreStartTime);
    metrics.filesRestored.set(filesRestored.longValue());
    metrics.bytesRestored.set(bytesRestored.longValue());
  }

  @Override
  public void close() {
  }

  /**
   * Deletes blob store contents for stores that were present in the last checkpoint but are no longer present in
   * job configs. In other words, cleans up stores that have been removed by users since the last deployment.
   *
   * This method blocks until all the necessary store contents and snapshot index blobs have been marked for deletion.
   */
  // TODO BLOCKER shesharm this method was only removing the index blob. It needs to remove the entire store, including
  // all of its contents. I fixed it, but please verify and add unit tests.
  private void deleteUnusedStoresFromBlobStore(String taskName, List<String> currentTaskStores,
      Map<String, Pair<String, SnapshotIndex>> initialStoreSnapshotIndexes,
      BlobStoreUtil blobStoreUtil, ExecutorService executor) {
    long startTime = System.nanoTime();
    List<CompletionStage<Void>> storeDeletionFutures = new ArrayList<>();
    initialStoreSnapshotIndexes.forEach((storeName, scmAndSnapshotIndex) -> {
      if (!currentTaskStores.contains(storeName)) {
        LOG.debug("Removing task: {} store: {} from blob store since it is no longer used.", taskName, storeName);
        DirIndex dirIndex = scmAndSnapshotIndex.getRight().getDirIndex();
        CompletionStage<Void> storeDeletionFuture =
            blobStoreUtil.cleanUpDir(dirIndex) // delete files and sub-dirs previously marked for removal
                .thenComposeAsync(v ->
                    blobStoreUtil.deleteDir(dirIndex), executor) // deleted files and dirs still present
                .thenComposeAsync(v -> blobStoreUtil.deleteSnapshotIndexBlob(
                    scmAndSnapshotIndex.getLeft()),
                    executor); // delete the snapshot index blob
        storeDeletionFutures.add(storeDeletionFuture);
      }
    });
    FutureUtil.allOf(storeDeletionFutures).join();
  }
}
