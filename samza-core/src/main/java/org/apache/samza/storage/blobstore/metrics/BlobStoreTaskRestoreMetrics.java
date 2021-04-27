package org.apache.samza.storage.blobstore.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;


public class BlobStoreTaskRestoreMetrics {
  // ToDo per-task throughput
  // Metrics
  public Timer restoreManagerInitTimeNs;
  public Timer getSnapshotIndexTimeNs;

  public Timer downloadNs;
  public Map<String, Timer> storeDownloadNs;
  public Timer downloadFileNs; // avg time for each file download
  public Gauge<Long> numFilesDownloaded;
  public Gauge<Long> totalBytesDownloaded; // total bytes downloaded in restore

  private final MetricsRegistry metricsRegistry;
  private final String group;

  public BlobStoreTaskRestoreMetrics(String group, MetricsRegistry metricsRegistry) {
    this.metricsRegistry = metricsRegistry;
    this.group = group;
    initMetrics();
  }

  private void initMetrics() {
    restoreManagerInitTimeNs = metricsRegistry.newTimer(group, "init-ns");
    getSnapshotIndexTimeNs = metricsRegistry.newTimer(group, "get-store-snapshot-index-ns");
    downloadNs = metricsRegistry.newTimer(group, "download-ns");
    storeDownloadNs = new ConcurrentHashMap<>();
    downloadFileNs = metricsRegistry.newTimer(group, "download-file-ns");
    numFilesDownloaded = metricsRegistry.newGauge(group, "num-files-downloaded", 0L);
    totalBytesDownloaded = metricsRegistry.newGauge(group, "total-bytes-downloaded", 0L);
  }

  public void registerSource(String storeName) {
    storeDownloadNs.putIfAbsent(storeName, metricsRegistry.newTimer(group, "download-ns"));
  }
}
