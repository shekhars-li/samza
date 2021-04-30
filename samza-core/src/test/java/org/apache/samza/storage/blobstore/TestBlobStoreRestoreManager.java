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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import com.linkedin.util.Pair;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import org.apache.commons.io.FileUtils;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.CheckpointV2;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.apache.samza.storage.StorageEngine;
import org.apache.samza.storage.StorageManagerUtil;
import org.apache.samza.storage.blobstore.index.DirIndex;
import org.apache.samza.storage.blobstore.index.SnapshotIndex;
import org.apache.samza.storage.blobstore.index.SnapshotMetadata;
import org.apache.samza.storage.blobstore.metrics.BlobStoreRestoreManagerMetrics;
import org.apache.samza.storage.blobstore.util.BlobStoreTestUtil;
import org.apache.samza.storage.blobstore.util.BlobStoreUtil;
import org.apache.samza.util.Clock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.anySetOf;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestBlobStoreRestoreManager {
  private static final Logger LOG = LoggerFactory.getLogger(TestBlobStoreRestoreManager.class);
  private static final ExecutorService EXECUTOR = MoreExecutors.newDirectExecutorService();

  // mock container - task - job models
  private final TaskModel taskModel = mock(TaskModel.class, RETURNS_DEEP_STUBS);
  private final Clock clock = mock(Clock.class);
  private final StorageManagerUtil storageManagerUtil = mock(StorageManagerUtil.class);
  private final BlobStoreUtil blobStoreUtil = mock(BlobStoreUtil.class);

  private final MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
  private final Counter counter = mock(Counter.class);
  private final Timer timer = mock(Timer.class);
  private final Gauge<Long> longGauge = mock(Gauge.class);
  private final Gauge<AtomicLong> atomicLongGauge = mock(Gauge.class);

  //job and store definition
  private final CheckpointId checkpointId = CheckpointId.deserialize("1234-567");
  private final String jobName = "testJobName";
  private final String jobId = "testJobID";
  private final String taskName = "testTaskName";
  private final String prevSnapshotIndexBlobId = "testPrevBlobId";
  private Map<String, StorageEngine> storeStorageEngineMap = new HashMap<>();
  private Map<String, String> mapConfig = new HashMap<>();
  private String restoreDirBasePath;

  // Remote and local snapshot definitions
  private Map<String, SnapshotIndex> testBlobStore = new HashMap<>();
  private Map<String, Pair<String, SnapshotIndex>> indexBlobIdAndLocalRemoteSnapshotsPair;
  private Map<String, String> testStoreNameAndSCMMap;

  private BlobStoreRestoreManager blobStoreRestoreManager;
  private BlobStoreRestoreManagerMetrics metrics;

  /**
   * Test restore handles logged / non-logged / durable / persistent stores correctly.
   * Test restore restores to the correct store directory depending on store type.
   * Test logic for checking if checkpoint directory is identical to remote snapshot.
   * Test that it ignores any files that are not present when upload is called (e.g. offset files).
   * Test restore handles stores with missing SCMs correctly.
   * Test restore handles multiple stores correctly.
   * Test restore always deletes main store dir.
   * Test restore uses previous checkpoint directory if identical to remote snapshot.
   * Test restore restores from remote snapshot if no previous checkpoint dir.
   * Test restore restores from remote snapshot if checkpoint dir not identical to remote snapshot.
   * Test restore recreates subdirs correctly.
   * Test restore recreates recursive subdirs correctly
   * Test restore creates empty files correctly.
   * Test restore creates empty dirs correctly.
   * Test restore creates for empty sub-dirs / recursive subdirs correctly.
   * Test restore restores multi-part file contents completely and in correct order.
   * Test restore verifies checksum for files restored if enabled.
   */

  @Before
  public void setup() throws Exception {
    when(clock.currentTimeMillis()).thenReturn(1234567L);
    // setup test local and remote snapshots
    indexBlobIdAndLocalRemoteSnapshotsPair = setupRemoteAndLocalSnapshots(true);
    // setup test store name and SCMs map
    testStoreNameAndSCMMap = setupTestStoreSCMMapAndStoreBackedFactoryConfig(indexBlobIdAndLocalRemoteSnapshotsPair);
    // setup: setup task backup manager with expected storeName->storageEngine map
    testStoreNameAndSCMMap.forEach((storeName, scm) -> storeStorageEngineMap.put(storeName, null));
    // Setup mock config and task model
    restoreDirBasePath = Files.createTempDirectory(BlobStoreTestUtil.TEMP_DIR_PREFIX).toString();
    mapConfig.putAll(new MapConfig(ImmutableMap.of("job.name", jobName, "job.id", jobId,
        "job.logged.store.base.dir", restoreDirBasePath)));
    Config config = new MapConfig(mapConfig);
    when(taskModel.getTaskName().getTaskName()).thenReturn(taskName);
    when(taskModel.getTaskMode()).thenReturn(TaskMode.Active);

    // Mock - return snapshot index for blob id from test blob store map
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    when(blobStoreUtil.getSnapshotIndex(captor.capture()))
        .then((Answer<CompletableFuture<SnapshotIndex>>) invocation -> {
          String blobId = invocation.getArgumentAt(0, String.class);
          return CompletableFuture.completedFuture(testBlobStore.get(blobId));
        });

    when(metricsRegistry.newCounter(anyString(), anyString())).thenReturn(counter);
    when(metricsRegistry.newGauge(anyString(), anyString(), anyLong())).thenReturn(longGauge);
    when(metricsRegistry.newGauge(anyString(), anyString(), any(AtomicLong.class))).thenReturn(atomicLongGauge);
    when(atomicLongGauge.getValue()).thenReturn(new AtomicLong());
    when(metricsRegistry.newTimer(anyString(), anyString())).thenReturn(timer);
    metrics = new BlobStoreRestoreManagerMetrics(metricsRegistry);

    blobStoreRestoreManager =
        new BlobStoreRestoreManager(taskModel, EXECUTOR, metrics, config, storageManagerUtil, blobStoreUtil,
            Files.createTempDirectory("logged-store-").toFile(), null);
  }

  @Test
  public void testRestoreHandlesStoresWithMissingCheckpointSCMCorrectly() {

  }

  @Test
  public void testRestoreHandlesMultipleStoresCorrectly() {

  }

  @Test
  public void testRestoreHandlesStoreTypesCorrectly() {
    // TODO clarify what correctly means in test name
    // logged, non-logged, persistent, durable stores.
  }

  @Test
  public void testRestoreDeletesStoreDir() throws IOException {
    Checkpoint checkpoint =
        new CheckpointV2(checkpointId, ImmutableMap.of(),
            ImmutableMap.of(StorageConfig.BLOB_STORE_STATE_BACKEND_FACTORY, testStoreNameAndSCMMap));

    String testStoreDir = Files.createTempDirectory(BlobStoreTestUtil.TEMP_DIR_PREFIX).toString();

    // mock: set task store dir to return test local store
    when(storageManagerUtil.getTaskStoreDir(any(File.class), anyString(), any(TaskName.class), any(TaskMode.class)))
        .thenReturn(new File(testStoreDir));

    // mock: do not complete any restore. Return immediately
    when(blobStoreUtil.restoreDir(any(), any()))
        .thenReturn(Collections.singletonList(CompletableFuture.completedFuture(null)));
    when(blobStoreUtil.areSameDir(anySet(), false)).thenReturn((u,v)->true);

    blobStoreRestoreManager.init(checkpoint);
    blobStoreRestoreManager.restore();

    // Verify the store dir is not present anymore and is deleted by restore.
    Assert.assertFalse(Files.exists(Paths.get(testStoreDir)));
  }

  @Test
  public void testRestoreRestoresRemoteSnapshotIfNoCheckpointDir() throws IOException {
    Checkpoint checkpoint =
        new CheckpointV2(checkpointId, ImmutableMap.of(),
            ImmutableMap.of(StorageConfig.BLOB_STORE_STATE_BACKEND_FACTORY, testStoreNameAndSCMMap));

    // mock: set task store dir to return corresponding test local store and create checkpoint dir
    ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);
    when(storageManagerUtil.getTaskStoreDir(any(File.class), stringCaptor.capture(), any(TaskName.class), any(TaskMode.class)))
        .then((Answer<File>) invocation -> {
          String storeName = invocation.getArgumentAt(1, String.class);
          String snapshotIndexBlobId = testStoreNameAndSCMMap.get(storeName);
          String storeDir = indexBlobIdAndLocalRemoteSnapshotsPair.get(snapshotIndexBlobId).getFirst();
          return new File(storeDir);
        });

    SortedSet<String> expectedStoreDirsRestored = new TreeSet<>(testStoreNameAndSCMMap.keySet());
    SortedSet<String> actualStoreDirsRestored = new TreeSet<>();
    // Mock: mock restoreDir call
    ArgumentCaptor<File> fileCaptor = ArgumentCaptor.forClass(File.class);
    when(blobStoreUtil.restoreDir(fileCaptor.capture(), any(DirIndex.class)))
        .then((Answer<List<CompletableFuture<Void>>>) invocation -> {
          File storeBaseDir = invocation.getArgumentAt(0, File.class);
          actualStoreDirsRestored.add(storeBaseDir.getPath());
          return Collections.singletonList(CompletableFuture.completedFuture(null));
        });

    // Mock areSameDir to always return true
    BiPredicate<File, DirIndex> mockPredicate = ((u, v) -> true);
    when(blobStoreUtil.areSameDir(anySetOf(String.class), false)).thenReturn(mockPredicate);

    blobStoreRestoreManager.init(checkpoint);
    blobStoreRestoreManager.restore();

    Assert.assertEquals(actualStoreDirsRestored, expectedStoreDirsRestored);
  }

  @Test
  public void testRestoreRestoresRemoteSnapshotIfCheckpointDirNotIdenticalToRemoteSnapshot() {
    // Track directory for post cleanup
    List<String> checkpointDirsToClean = new ArrayList<>();

    Map.Entry<String, String> testStoreNameSCMPair = testStoreNameAndSCMMap.entrySet().iterator().next();
    Checkpoint checkpoint =
        new CheckpointV2(checkpointId, ImmutableMap.of(),
            ImmutableMap.of(StorageConfig.BLOB_STORE_STATE_BACKEND_FACTORY,
                ImmutableMap.of(testStoreNameSCMPair.getKey(), testStoreNameSCMPair.getValue())));

    // mock: set task store dir to return corresponding test local store and create checkpoint dir
    ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);
    when(storageManagerUtil.getTaskStoreDir(any(File.class), stringCaptor.capture(), any(TaskName.class), any(TaskMode.class)))
        .then((Answer<File>) invocation -> {
          String storeName = invocation.getArgumentAt(1, String.class);
          String snapshotIndexBlobId = testStoreNameAndSCMMap.get(storeName);
          String storeDir = indexBlobIdAndLocalRemoteSnapshotsPair.get(snapshotIndexBlobId).getFirst();
          try {
            BlobStoreTestUtil.createTestCheckpointDirectory(storeDir, checkpointId.serialize()); // create test checkpoint dir
            checkpointDirsToClean.add(storeDir + "-" + checkpointId.serialize()); // track checkpoint dir to cleanup later
          } catch (IOException e) {
            Assert.fail("Couldn't create checkpoint directory. Test failed.");
          }
          return new File(storeDir);
        });

    String expectedStoreDirsRestored = testStoreNameSCMPair.getKey();
    final String[] actualStoreDirsRestored = new String[1];
    // Mock: mock restoreDir call
    ArgumentCaptor<File> fileCaptor = ArgumentCaptor.forClass(File.class);
    when(blobStoreUtil.restoreDir(fileCaptor.capture(), any(DirIndex.class)))
        .then((Answer<List<CompletableFuture<Void>>>) invocation -> {
          File storeBaseDir = invocation.getArgumentAt(0, File.class);
          actualStoreDirsRestored[0] = storeBaseDir.getPath();
          return Collections.singletonList(CompletableFuture.completedFuture(null));
        });

    // Mock areSameDir to always return false and then true
    BiPredicate<File, DirIndex> mockPredicateTrue = ((u, v) -> true);
    BiPredicate<File, DirIndex> mockPredicateFalse = ((u, v) -> false);
    // areSameDir returns false on first call (to check if checkpoint dir is same as remote) and then true for second
    // call (to check if the restore was done correctly).
    when(blobStoreUtil.areSameDir(anySet(), false)).thenReturn(mockPredicateFalse, mockPredicateTrue);

    blobStoreRestoreManager.init(checkpoint);
    blobStoreRestoreManager.restore();

    Assert.assertEquals(actualStoreDirsRestored[0], expectedStoreDirsRestored);

    // cleanup
    checkpointDirsToClean.forEach(path -> {
      try {
        FileUtils.deleteDirectory(new File(path));
      } catch (IOException exception) {
        Assert.fail("Failed to cleanup temporary checkpoint dirs.");
      }
    });
  }

  @Test
  public void testRestoreRetainsPreviousCheckpointDirIfIdenticalToRemoteSnapshot() {
    // Track directory for post cleanup
    List<String> checkpointDirsToClean = new ArrayList<>();

    Map.Entry<String, String> testStoreNameSCMPair = testStoreNameAndSCMMap.entrySet().iterator().next();
    Checkpoint checkpoint =
        new CheckpointV2(checkpointId, ImmutableMap.of(),
            ImmutableMap.of(StorageConfig.BLOB_STORE_STATE_BACKEND_FACTORY,
                ImmutableMap.of(testStoreNameSCMPair.getKey(), testStoreNameSCMPair.getValue())));

    // mock: set task store dir to return corresponding test local store and create checkpoint dir
    ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);
    when(storageManagerUtil.getTaskStoreDir(any(File.class), stringCaptor.capture(), any(TaskName.class), any(TaskMode.class)))
        .then((Answer<File>) invocation -> {
          String storeName = invocation.getArgumentAt(1, String.class);
          String snapshotIndexBlobId = testStoreNameAndSCMMap.get(storeName);
          String storeDir = indexBlobIdAndLocalRemoteSnapshotsPair.get(snapshotIndexBlobId).getFirst();
          try {
            BlobStoreTestUtil.createTestCheckpointDirectory(storeDir, checkpointId.serialize()); // create test checkpoint dir
            checkpointDirsToClean.add(storeDir + "-" + checkpointId.serialize()); // track checkpoint dir to cleanup later
          } catch (IOException e) {
            Assert.fail("Couldn't create checkpoint directory. Test failed.");
          }
          return new File(storeDir);
        });

    // Mock areSameDir to always return false and then true
    BiPredicate<File, DirIndex> mockPredicateTrue = ((u, v) -> true);
    when(blobStoreUtil.areSameDir(anySetOf(String.class), false)).thenReturn(mockPredicateTrue);

    blobStoreRestoreManager.init(checkpoint);
    blobStoreRestoreManager.restore();

    verify(blobStoreUtil, never()).restoreDir(any(), any());

    // cleanup
    checkpointDirsToClean.forEach(path -> {
      try {
        FileUtils.deleteDirectory(new File(path));
      } catch (IOException exception) {
        Assert.fail("Failed to cleanup temporary checkpoint dirs.");
      }
    });
  }

  @Test
  public void testInitRemovesStoresDeletedFromConfig() throws IOException {
    // Setup: removing 1 store from config
    Map.Entry<String, String> entry =
        new MapConfig(mapConfig).regexSubset("stores.*.factory").entrySet().iterator().next();
    mapConfig.remove(entry.getKey());
    Config config = new MapConfig(mapConfig);
    blobStoreRestoreManager =
        new BlobStoreRestoreManager(taskModel, EXECUTOR, metrics, config, storageManagerUtil, blobStoreUtil,
            Files.createTempDirectory("logged-store-").toFile(), null);

    String storeRemovedFromConfig =
        entry.getKey().substring(entry.getKey().indexOf('.') + 1, entry.getKey().lastIndexOf('.'));
    // Setup expected result: The blob ID of scm from storeName->scm removed in previous step
    String expectedSnapshotIndexBlobIdDeleted = testStoreNameAndSCMMap.get(storeRemovedFromConfig);

    DirIndex expectedDirIndexCleanedAndDeleted =
        indexBlobIdAndLocalRemoteSnapshotsPair.get(expectedSnapshotIndexBlobIdDeleted).getSecond().getDirIndex();

    // Actual result
    final String[] actualSnapshotIndexBlobIdDeleted = new String[1];
    final List<DirIndex> actualDirIndexCleanedAndDeleted = new ArrayList<>();

    // Mock - capture deleted snapshot index
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    when(blobStoreUtil.deleteSnapshotIndexBlob(captor.capture()))
        .then((Answer<CompletableFuture<Void>>) invocation -> {
          actualSnapshotIndexBlobIdDeleted[0] = invocation.getArgumentAt(0, String.class);
          return CompletableFuture.completedFuture(null);
        });
    ArgumentCaptor<DirIndex> dirIndexCaptor = ArgumentCaptor.forClass(DirIndex.class);
    // Mock - capture cleaned up DirIndex
    when(blobStoreUtil.cleanUpDir(dirIndexCaptor.capture()))
        .then((Answer<CompletableFuture<Void>>) invocation -> {
          DirIndex dirIndex = invocation.getArgumentAt(0, DirIndex.class);
          actualDirIndexCleanedAndDeleted.add(dirIndex);
          return CompletableFuture.completedFuture(null);
        });
    // Mock - capture deleted DirIndex
    when(blobStoreUtil.deleteDir(dirIndexCaptor.capture()))
        .then((Answer<CompletableFuture<Void>>) invocation -> {
          DirIndex dirIndex = invocation.getArgumentAt(0, DirIndex.class);
          actualDirIndexCleanedAndDeleted.add(dirIndex);
          return CompletableFuture.completedFuture(null);
        });

    // Execute
    Checkpoint checkpoint =
        new CheckpointV2(CheckpointId.create(), new HashMap<>(),
            ImmutableMap.of(StorageConfig.BLOB_STORE_STATE_BACKEND_FACTORY, testStoreNameAndSCMMap));
    blobStoreRestoreManager.init(checkpoint);

    // Verify
    Assert.assertEquals(actualSnapshotIndexBlobIdDeleted[0], expectedSnapshotIndexBlobIdDeleted);
    // Verify: same dir Index is first cleaned and then deleted.
    Assert.assertEquals(actualDirIndexCleanedAndDeleted.get(0), expectedDirIndexCleanedAndDeleted);
    Assert.assertEquals(actualDirIndexCleanedAndDeleted.get(1), expectedDirIndexCleanedAndDeleted);

    // cleanup: add back the store entry removed earlier from config
    mapConfig.put(entry.getKey(), entry.getValue());
  }

  private Map<String, String> setupTestStoreSCMMapAndStoreBackedFactoryConfig(Map<String,
      Pair<String, SnapshotIndex>> indexBlobIdAndRemoteAndLocalSnapshotMap) {
    Map<String, String> storeNameSCMMap = new HashMap<>();
    indexBlobIdAndRemoteAndLocalSnapshotMap
        .forEach((blobId, localRemoteSnapshots) -> {
          mapConfig.put("stores."+localRemoteSnapshots.getFirst()+".factory", BlobStoreStateBackendFactory.class.getName());
          mapConfig.put("stores."+localRemoteSnapshots.getFirst()+".state.backend.backup.factories", BlobStoreStateBackendFactory.class.getName());
          storeNameSCMMap.put(localRemoteSnapshots.getFirst(), blobId);
        });
    return storeNameSCMMap;
  }

  private Map<String, Pair<String, SnapshotIndex>> setupRemoteAndLocalSnapshots(boolean addPrevCheckpoints) throws IOException {
    testBlobStore = new HashMap<>(); // reset blob store
    Map<String, Pair<String, SnapshotIndex>> indexBlobIdAndRemoteAndLocalSnapshotMap = new HashMap<>();
    List<String> localSnapshots = new ArrayList<>();
    List<String> previousRemoteSnapshots = new ArrayList<>();

    localSnapshots.add("[a, c, z/1, y/2, p/m/3, q/n/4]");
    previousRemoteSnapshots.add("[a, b, z/1, x/5, p/m/3, r/o/6]");

    localSnapshots.add("[a, c, z/1, y/1, p/m/1, q/n/1]");
    previousRemoteSnapshots.add("[a, z/1, p/m/1]");

    localSnapshots.add("[z/i/1, y/j/1]");
    previousRemoteSnapshots.add("[z/i/1, x/k/1]");

    // setup local and corresponding remote snapshots
    for (int i=0; i<localSnapshots.size(); i++) {
      Path localSnapshot = BlobStoreTestUtil.createLocalDir(localSnapshots.get(i));
      String testLocalSnapshot = localSnapshot.toAbsolutePath().toString();
      DirIndex dirIndex = BlobStoreTestUtil.createDirIndex(localSnapshots.get(i));
      SnapshotMetadata snapshotMetadata = new SnapshotMetadata(checkpointId, jobName, jobId, taskName, testLocalSnapshot);
      Optional<String> prevCheckpointId = Optional.empty();
      if (addPrevCheckpoints) {
        prevCheckpointId = Optional.of(prevSnapshotIndexBlobId + "-" + i);
        DirIndex prevDirIndex = BlobStoreTestUtil.createDirIndex(previousRemoteSnapshots.get(i));
        testBlobStore.put(prevCheckpointId.get(),
            new SnapshotIndex(clock.currentTimeMillis(), snapshotMetadata, prevDirIndex, Optional.empty()));
      }
      SnapshotIndex testRemoteSnapshot =
          new SnapshotIndex(clock.currentTimeMillis(), snapshotMetadata, dirIndex, prevCheckpointId);
      indexBlobIdAndRemoteAndLocalSnapshotMap.put("blobId-" + i, Pair.of(testLocalSnapshot, testRemoteSnapshot));
      testBlobStore.put("blobId-" + i, testRemoteSnapshot);
    }
    return indexBlobIdAndRemoteAndLocalSnapshotMap;
  }
}
