package org.apache.samza.storage.blobstore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsHelper;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;


public class BlobStoreMetrics extends MetricsHelper {
  private final String prefix;

  // Metrics
  // backup manager - init
  public Counter backupManagerInits;
  public Timer backupManagerInitTimeNs;

  // backup manager - upload
  public Counter uploads;
  public Timer uploadNs;
  public Map<String, Timer> taskStoreUploadTimeNs;
  public Map<String, Timer> taskStoreDirDiffTimeNs;

  // backup manager - cleanup
  public Counter cleanups;
  public Timer cleanupNs;
//  public Gauge<Long> ttlRemoved = newGauge("cleanup-ttl-removed", 0L);
//  public Gauge<Long> remoteSnapshotsCleaned = newGauge("cleanup-remote-snapshots-cleaned", 0L);
//  public Gauge<Long> prevRemoteSnapshotsRemoved = newGauge("cleanup-remote-snapshots-removed", 0L);
  public Map<String, Gauge<Long>> taskStoreFilesToAdd;
  public Map<String, Gauge<Long>> taskStoreFilesToRetain;
  public Map<String, Gauge<Long>> taskStoreFilesToRemove;
  public Map<String, Gauge<Long>> taskStoreSubDirsToAdd;
  public Map<String, Gauge<Long>> taskStoreSubDirsToRetain;
  public Map<String, Gauge<Long>> taskStoreSubDirsToRemove;
  public Map<String, Timer> taskStorePutDirNs;

  // Restore manager - init
  public Counter restoreManagerInits;
  public Timer restoreManagerInitTimeNs;
  public Counter deleteUnusedStores;
  public Gauge<Long> storesRemovedFromConfig;
  public Timer unusedStoreDeletedTimeNs;

  // Restore manager - restore
  public Counter restores;
  public Timer restoreTimeNs;

//  // BlobStoreBackendUtil
//  public Gauge<Long> nullCheckpoints = newGauge("null-checkpoint", 0L);
//  public Gauge<Long> v1Checkpoints = newGauge("v1-checkpoint", 0L);
//  public Timer getSnapshotIndexTimeNs = newTimer("get-store-snapshot-index-time-ns");
//
//  // BlobStoreUtil
//  public Counter putFiles = newCounter("put-file-call"); // rate of file upload
//  public Gauge<Long> invalidFile = newGauge("put-file-null-invalid-file", 0L);
//  public Gauge<Long> unclosedStream = newGauge("put-file-stream-error", 0L);
//  public Timer putFileNs = newTimer("put-file-time-ns"); // avg time for each file upload
//  public Timer avgFileSizeBytes = newTimer("avg-put-file-size-bytes"); // avg size of each file uploaded
//
//  public Counter putSnapshotIndices = newCounter("put-snapshot-index-call");

  public BlobStoreMetrics(MetricsRegistry metricsRegistry, String prefix) {
    super(metricsRegistry);
    this.prefix = prefix;
    backupManagerInits = newCounter("backup-manager-init-call");
    backupManagerInitTimeNs = newTimer("backup-manager-init-time-ns");
    uploads = newCounter("uploads-call");
    uploadNs = newTimer("backup-manager-upload-time-ns");
    taskStoreUploadTimeNs = new ConcurrentHashMap<>();
    taskStoreDirDiffTimeNs = new ConcurrentHashMap<>();
    cleanups = newCounter("cleanup-call");
    cleanupNs = newTimer("backup-manager-cleanup-time-ns");
    taskStoreFilesToAdd = new ConcurrentHashMap<>();
    taskStoreFilesToRetain = new ConcurrentHashMap<>();
    taskStoreFilesToRemove = new ConcurrentHashMap<>();
    taskStoreSubDirsToAdd = new ConcurrentHashMap<>();
    taskStoreSubDirsToRetain = new ConcurrentHashMap<>();
    taskStoreSubDirsToRemove = new ConcurrentHashMap<>();
    taskStorePutDirNs = new ConcurrentHashMap<>();
    restoreManagerInits = newCounter("restore-manager-inits-call");
    restoreManagerInitTimeNs = newTimer("restore-manager-init-time-ns");
    deleteUnusedStores = newCounter("restore-manager-delete-unused-stores-call");
    storesRemovedFromConfig = newGauge("restore-manager-stores-deleted-config", 0L);
    unusedStoreDeletedTimeNs = newTimer("restore-manager-unused-store-delete-time-ns");
    restores = newCounter("restore-manager-restore-call");
    restoreTimeNs = newTimer("restore-manager-restore-time-ns");
  }

  @Override
  public String getPrefix() {
    return prefix;
  }



  public void registerSource(String storeName) {
    taskStoreUploadTimeNs.putIfAbsent(storeName, newTimer(String.format("%s-upload-time-ns", storeName)));
    taskStoreDirDiffTimeNs.putIfAbsent(storeName, newTimer(String.format("%s-dir-diff-time-ns", storeName)));
    taskStorePutDirNs.putIfAbsent(storeName, newTimer((String.format("%s-put-dir-time-ns", storeName))));
    taskStoreFilesToAdd.putIfAbsent(storeName, newGauge(String.format("%s-files-to-upload", storeName), 0L));
    taskStoreFilesToRetain.putIfAbsent(storeName, newGauge(String.format("%s-files-to-retain", storeName), 0L));
    taskStoreFilesToRemove.putIfAbsent(storeName, newGauge(String.format("%s-files-to-remove", storeName), 0L));
    taskStoreSubDirsToAdd.putIfAbsent(storeName, newGauge(String.format("%s-sub-dirs-to-add", storeName), 0L));
    taskStoreSubDirsToRetain.putIfAbsent(storeName, newGauge(String.format("%s-sub-dirs-to-retain", storeName), 0L));
    taskStoreSubDirsToRemove.putIfAbsent(storeName, newGauge(String.format("%s-sub-dirs-to-remove", storeName), 0L));
  }
}
