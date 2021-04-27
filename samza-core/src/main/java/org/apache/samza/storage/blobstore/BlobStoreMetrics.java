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
  public Map<String, Timer> storeUploadTimeNs;
  public Map<String, Timer> storeDirDiffTimeNs;

  public Map<String, Timer> storeUploadNs;
  public Map<String, Gauge<Long>> storeFilesToUpload;
  public Map<String, Gauge<Long>> storeFilesToRetain;
  public Map<String, Gauge<Long>> storeFilesToRemove;
  public Map<String, Gauge<Long>> storeSubDirsToUpload;
  public Map<String, Gauge<Long>> storeSubDirsToRetain;
  public Map<String, Gauge<Long>> storeSubDirsToRemove;
  public Map<String, Gauge<Long>> storeBytesToUpload;
  public Map<String, Gauge<Long>> storeBytesToRetain;
  public Map<String, Gauge<Long>> storeBytesToRemove;

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
    backupManagerInitTimeNs = metricsRegistry.newTimer(group, "backup-manager-init-ns");
    uploadNs = metricsRegistry.newTimer(group, "backup-manager-upload-ns");
    storeUploadTimeNs = new ConcurrentHashMap<>();
    storeDirDiffTimeNs = new ConcurrentHashMap<>();
    storeFilesToUpload = new ConcurrentHashMap<>();
    storeFilesToRetain = new ConcurrentHashMap<>();
    storeFilesToRemove = new ConcurrentHashMap<>();
    storeSubDirsToUpload = new ConcurrentHashMap<>();
    storeSubDirsToRetain = new ConcurrentHashMap<>();
    storeSubDirsToRemove = new ConcurrentHashMap<>();
    storeBytesToUpload = new ConcurrentHashMap<>();
    storeBytesToRetain = new ConcurrentHashMap<>();
    storeBytesToRemove = new ConcurrentHashMap<>();
    cleanupNs = metricsRegistry.newTimer(group, "cleanup-ns");
    restoreManagerInitTimeNs = metricsRegistry.newTimer(group, "restore-manager-init-ns");
    getSnapshotIndexTimeNs = metricsRegistry.newTimer(group, "get-store-snapshot-index-ns");
    storeUploadNs = new ConcurrentHashMap<>();
    putDirNs = metricsRegistry.newTimer(group, "put-dir-ns");
    numFilesUploaded = metricsRegistry.newGauge(group, "num-files-uploaded", 0L);
    numFilesDownloaded = metricsRegistry.newGauge(group, "num-files-downloaded", 0L);
    putFileNs = metricsRegistry.newTimer(group,"put-file-ns");
    avgPutFileSizeBytes = metricsRegistry.newTimer(group,"avg-put-file-size-bytes");
    totalBytesUploaded = metricsRegistry.newGauge(group, "bytes-uploaded", 0L);
    storeRestoreDirNs = new ConcurrentHashMap<>();
    getFileNs = metricsRegistry.newTimer(group, "get-file-ns");
  }

  public void registerSource(String storeName) {
    storeUploadTimeNs.putIfAbsent(storeName,
        metricsRegistry.newTimer(group, String.format("%s-upload-ns", storeName)));
    storeDirDiffTimeNs.putIfAbsent(storeName,
        metricsRegistry.newTimer(group, String.format("%s-dir-diff-ns", storeName)));
    storeUploadNs.putIfAbsent(storeName,
        metricsRegistry.newTimer(group, String.format("%s-upload-dir-ns", storeName)));
    storeFilesToUpload.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-files-to-upload", storeName), 0L));
    storeFilesToRetain.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-files-to-retain", storeName), 0L));
    storeFilesToRemove.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-files-to-remove", storeName), 0L));
    storeBytesToUpload.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-bytes-to-upload", storeName), 0L));
    storeBytesToRetain.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-bytes-to-retain", storeName), 0L));
    storeBytesToRemove.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-bytes-to-remove", storeName), 0L));
    storeSubDirsToUpload.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-sub-dirs-to-upload", storeName), 0L));
    storeSubDirsToRetain.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-sub-dirs-to-retain", storeName), 0L));
    storeSubDirsToRemove.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-sub-dirs-to-remove", storeName), 0L));
    storeRestoreDirNs.putIfAbsent(storeName, metricsRegistry.newGauge(group, "store-restore-ns", 0L));
  }
}
