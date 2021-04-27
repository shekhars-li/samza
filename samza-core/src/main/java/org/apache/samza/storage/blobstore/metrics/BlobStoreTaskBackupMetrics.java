package org.apache.samza.storage.blobstore.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;


public class BlobStoreTaskBackupMetrics {
  // ToDo per-task throughput
  // Metrics
  public Timer backupManagerInitTimeNs;
  public Timer uploadNs;
  public Map<String, Timer> storeUploadNs;

  public Map<String, Timer> storeDirDiffTimeNs;
  public Map<String, Gauge<Long>> storeFilesToUpload;
  public Map<String, Gauge<Long>> storeFilesToRetain;
  public Map<String, Gauge<Long>> storeFilesToRemove;
  public Map<String, Gauge<Long>> storeSubDirsToUpload;
  public Map<String, Gauge<Long>> storeSubDirsToRetain;
  public Map<String, Gauge<Long>> storeSubDirsToRemove;
  public Map<String, Gauge<Long>> storeBytesToUpload;
  public Map<String, Gauge<Long>> storeBytesToRetain;
  public Map<String, Gauge<Long>> storeBytesToRemove;

  public Timer cleanupNs;

  public Gauge<Long> numFilesUploaded;

  public Timer uploadFileNs; // avg time for each file upload
  // ToDO move to SamzaHistogram
  public Timer uploadFileSizeBytes; // avg size of each file uploaded
  public Gauge<Long> totalBytesUploaded; // total bytes uploaded in backup - across all stores

  private final MetricsRegistry metricsRegistry;
  private final String group;

  public BlobStoreTaskBackupMetrics(String group, MetricsRegistry metricsRegistry) {
    this.metricsRegistry = metricsRegistry;
    this.group = group;
    initMetrics();
  }

  private void initMetrics() {
    backupManagerInitTimeNs = metricsRegistry.newTimer(group, "init-ns");
    uploadNs = metricsRegistry.newTimer(group, "upload-ns");
    storeUploadNs = new ConcurrentHashMap<>();
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
    numFilesUploaded = metricsRegistry.newGauge(group, "num-files-uploaded", 0L);
    uploadFileNs = metricsRegistry.newTimer(group,"upload-file-ns");
    uploadFileSizeBytes = metricsRegistry.newTimer(group,"upload-file-size-bytes");
    totalBytesUploaded = metricsRegistry.newGauge(group, "total-bytes-uploaded", 0L);
  }

  public void registerSource(String storeName) {
    storeUploadNs.putIfAbsent(storeName,
        metricsRegistry.newTimer(group, String.format("%s-upload-ns", storeName)));
    storeDirDiffTimeNs.putIfAbsent(storeName,
        metricsRegistry.newTimer(group, String.format("%s-dir-diff-ns", storeName)));
    storeFilesToUpload.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-files-to-upload", storeName), 0L));
    storeFilesToRetain.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-files-to-retain", storeName), 0L));
    storeFilesToRemove.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-files-to-remove", storeName), 0L));
    storeSubDirsToUpload.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-sub-dirs-to-upload", storeName), 0L));
    storeSubDirsToRetain.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-sub-dirs-to-retain", storeName), 0L));
    storeSubDirsToRemove.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-sub-dirs-to-remove", storeName), 0L));
    storeBytesToUpload.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-bytes-to-upload", storeName), 0L));
    storeBytesToRetain.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-bytes-to-retain", storeName), 0L));
    storeBytesToRemove.putIfAbsent(storeName,
        metricsRegistry.newGauge(group, String.format("%s-bytes-to-remove", storeName), 0L));
  }
}
