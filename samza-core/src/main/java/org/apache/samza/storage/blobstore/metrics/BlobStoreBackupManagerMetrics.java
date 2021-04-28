package org.apache.samza.storage.blobstore.metrics;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;


public class BlobStoreBackupManagerMetrics {
  private final String group;
  private final MetricsRegistry metricsRegistry;

  // ToDo per-task throughput
  public final Gauge<Long> initNs;

  public final Timer uploadNs;
  public final Gauge<Long> filesUploaded;
  public final Gauge<Long> bytesUploaded;

  // per store breakdowns
  public final Map<String, Timer> storeDirDiffNs;
  public final Map<String, Timer> storeUploadNs;

  public final Map<String, Gauge<Long>> storeFilesToUpload;
  public final Map<String, Gauge<Long>> storeFilesToRetain;
  public final Map<String, Gauge<Long>> storeFilesToRemove;
  public final Map<String, Gauge<Long>> storeSubDirsToUpload;
  public final Map<String, Gauge<Long>> storeSubDirsToRetain;
  public final Map<String, Gauge<Long>> storeSubDirsToRemove;
  public final Map<String, Gauge<Long>> storeBytesToUpload;
  public final Map<String, Gauge<Long>> storeBytesToRetain;
  public final Map<String, Gauge<Long>> storeBytesToRemove;

  public final Gauge<Long> filesYetToUpload;
  public final Gauge<Long> bytesYetToUpload;

  public final Counter uploadMbps;

  public final Timer cleanupNs;

  // ToDO move to SamzaHistogram
  public final Timer avgFileUploadNs; // avg time for each file uploaded
  public final Timer avgFileSizeBytes; // avg size of each file uploaded

  public BlobStoreBackupManagerMetrics(String group, MetricsRegistry metricsRegistry) {
    this.group = group;
    this.metricsRegistry = metricsRegistry;

    this.initNs = metricsRegistry.newGauge(group, "init-ns", 0L);

    this.uploadNs = metricsRegistry.newTimer(group, "upload-ns");
    this.filesUploaded = metricsRegistry.newGauge(group, "files-uploaded", 0L);
    this.bytesUploaded = metricsRegistry.newGauge(group, "bytes-uploaded", 0L);

    this.storeDirDiffNs = new ConcurrentHashMap<>();
    this.storeUploadNs = new ConcurrentHashMap<>();

    this.storeFilesToUpload = new ConcurrentHashMap<>();
    this.storeFilesToRetain = new ConcurrentHashMap<>();
    this.storeFilesToRemove = new ConcurrentHashMap<>();
    this.storeSubDirsToUpload = new ConcurrentHashMap<>();
    this.storeSubDirsToRetain = new ConcurrentHashMap<>();
    this.storeSubDirsToRemove = new ConcurrentHashMap<>();
    this.storeBytesToUpload = new ConcurrentHashMap<>();
    this.storeBytesToRetain = new ConcurrentHashMap<>();
    this.storeBytesToRemove = new ConcurrentHashMap<>();

    this.filesYetToUpload = metricsRegistry.newGauge(group, "files-yet-to-upload", 0L);
    this.bytesYetToUpload = metricsRegistry.newGauge(group, "bytes-yet-to-upload", 0L);

    this.uploadMbps = metricsRegistry.newCounter(group, "upload-mbps");

    this.cleanupNs = metricsRegistry.newTimer(group, "cleanup-ns");

    this.avgFileUploadNs = metricsRegistry.newTimer(group,"avg-file-upload-ns");
    this.avgFileSizeBytes = metricsRegistry.newTimer(group,"avg-file-size-bytes");
  }

  public void initStoreMetrics(Collection<String> storeNames) {
    for (String storeName: storeNames) {
      storeDirDiffNs.putIfAbsent(storeName,
          metricsRegistry.newTimer(group, String.format("%s-dir-diff-ns", storeName)));
      storeUploadNs.putIfAbsent(storeName,
          metricsRegistry.newTimer(group, String.format("%s-upload-ns", storeName)));

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
}
