package org.apache.samza.storage.blobstore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;


public class BlobStoreMetrics {
  // name based on ops - upload/restore
  // ToDo per-task throughput
  // Metrics
  // backup manager - init
  public Timer backupManagerInitTimeNs;

  // backup manager - upload
  public Timer uploadNs;
  public Map<String, Timer> taskStoreUploadTimeNs;
  public Map<String, Timer> taskStoreDirDiffTimeNs;

  public Map<String, Timer> taskStorePutDirNs;

  public Map<String, Gauge<Long>> taskStoreFilesToUpload;
  public Map<String, Gauge<Long>> taskStoreFilesToRetain;
  public Map<String, Gauge<Long>> taskStoreFilesToRemove;
  public Map<String, Gauge<Long>> taskStoreSubDirsToAdd;
  public Map<String, Gauge<Long>> taskStoreSubDirsToRetain;
  public Map<String, Gauge<Long>> taskStoreSubDirsToRemove;
  public Map<String, Gauge<Long>> taskStoreBytesToUpload;
  public Map<String, Gauge<Long>> taskStoreBytesToRetain;
  public Map<String, Gauge<Long>> taskStoreBytesToRemove;

  // backup manager - cleanup
  public Timer cleanupNs;

  // Restore manager - init
  public Timer restoreManagerInitTimeNs;

  // BlobStoreBackendUtil
  public Timer getSnapshotIndexTimeNs;

  // BlobStoreUtil // gets with restores
  public Timer putDirNs;
  public Map<String, Gauge<Long>> storeRestoreDirNs;
  public Gauge<Long> numFilesUploaded;
  public Gauge<Long> numFilesDownloaded;
  public Timer putFileNs; // avg time for each file upload
  public Timer getFileNs; // avg time for each file download
  public Timer avgPutFileSizeBytes; // avg size of each file uploaded -> ToDO Use SamzaHistogram p50, p90, p99
  public Gauge<Long> totalBytesUploaded; // total bytes uploaded in backup
  public Gauge<Long> totalBytesDownloaded; // total bytes downloaded in restore

  private final MetricsRegistry metricsRegistry;
  private final String group;

  public BlobStoreMetrics(String group, MetricsRegistry metricsRegistry) {
    this.metricsRegistry = metricsRegistry;
    this.group = group;
    initMetrics();
  }

  private void initMetrics() {
    backupManagerInitTimeNs = metricsRegistry.newTimer(group, "backup-manager-init-time-ns");
    uploadNs = metricsRegistry.newTimer(group, "backup-manager-upload-time-ns");
    taskStoreUploadTimeNs = new ConcurrentHashMap<>();
    taskStoreDirDiffTimeNs = new ConcurrentHashMap<>();
    taskStoreFilesToUpload = new ConcurrentHashMap<>();
    taskStoreFilesToRetain = new ConcurrentHashMap<>();
    taskStoreFilesToRemove = new ConcurrentHashMap<>();
    taskStoreSubDirsToAdd = new ConcurrentHashMap<>();
    taskStoreSubDirsToRetain = new ConcurrentHashMap<>();
    taskStoreSubDirsToRemove = new ConcurrentHashMap<>();
    taskStoreBytesToUpload = new ConcurrentHashMap<>();
    taskStoreBytesToRetain = new ConcurrentHashMap<>();
    taskStoreBytesToRemove = new ConcurrentHashMap<>();
    cleanupNs = metricsRegistry.newTimer(group, "backup-manager-cleanup-time-ns");
    restoreManagerInitTimeNs = metricsRegistry.newTimer(group, "restore-manager-init-time-ns");
    getSnapshotIndexTimeNs = metricsRegistry.newTimer(group, "get-store-snapshot-index-time-ns");
    taskStorePutDirNs = new ConcurrentHashMap<>();
    putDirNs = metricsRegistry.newTimer(group, "put-dir-time-ns");
    numFilesUploaded = metricsRegistry.newGauge(group, "count-files-uploaded", 0L);
    numFilesDownloaded = metricsRegistry.newGauge(group, "count-files-downloaded", 0L);
    putFileNs = metricsRegistry.newTimer(group,"put-file-time-ns");
    avgPutFileSizeBytes = metricsRegistry.newTimer(group,"avg-put-file-size-bytes");
    totalBytesUploaded = metricsRegistry.newGauge(group, "total-size-files-uploaded-bytes", 0L);
    storeRestoreDirNs = new ConcurrentHashMap<>();
  }

  public void registerSource(String storeName) {
    taskStoreUploadTimeNs.putIfAbsent(storeName,
        metricsRegistry.newTimer(group, String.format("%s-upload-time-ns", storeName)));
    taskStoreDirDiffTimeNs.putIfAbsent(storeName,
        metricsRegistry.newTimer(group, String.format("%s-dir-diff-time-ns", storeName)));
    taskStorePutDirNs.putIfAbsent(storeName,
        metricsRegistry.newTimer(group, String.format("%s-put-dir-time-ns", storeName)));
    taskStoreFilesToUpload.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-files-to-upload", storeName), 0L));
    taskStoreFilesToRetain.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-files-to-retain", storeName), 0L));
    taskStoreFilesToRemove.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-files-to-remove", storeName), 0L));
    taskStoreBytesToUpload.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-bytes-to-upload", storeName), 0L));
    taskStoreBytesToRetain.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-bytes-to-retain", storeName), 0L));
    taskStoreBytesToRemove.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-bytes-to-remove", storeName), 0L));
    taskStoreSubDirsToAdd.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-sub-dirs-to-add", storeName), 0L));
    taskStoreSubDirsToRetain.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-sub-dirs-to-retain", storeName), 0L));
    taskStoreSubDirsToRemove.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-sub-dirs-to-remove", storeName), 0L));
    storeRestoreDirNs.putIfAbsent(storeName, metricsRegistry.newGauge(group, "restore-dir-time-ns", 0L));
  }
}
