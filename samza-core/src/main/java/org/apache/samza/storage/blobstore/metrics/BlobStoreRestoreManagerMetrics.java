package org.apache.samza.storage.blobstore.metrics;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;


public class BlobStoreRestoreManagerMetrics {
  private final String group;
  private final MetricsRegistry metricsRegistry;

  // ToDo per-task throughput
  public final Gauge<Long> initNs;
  public final Gauge<Long> getSnapshotIndexNs;

  public final Gauge<Long> restoreNs;
  public final Gauge<Long> filesRestored;
  public final Gauge<Long> bytesRestored; // total bytes downloaded in restore

  public final Gauge<Long> filesYetToRestore;
  public final Gauge<Long> bytesYetToRestore;

  public final Counter downloadMbps;

  // per store breakdowns
  public final Map<String, Gauge<Long>> storeRestoreNs;

  // ToDO move to SamzaHistogram
  public final Timer avgFileRestoreNs; // avg time for each file restored

  public BlobStoreRestoreManagerMetrics(String group, MetricsRegistry metricsRegistry) {
    this.group = group;
    this.metricsRegistry = metricsRegistry;

    this.initNs = metricsRegistry.newGauge(group, "init-ns", 0L);
    this.getSnapshotIndexNs = metricsRegistry.newGauge(group, "get-snapshot-index-ns", 0L);

    this.restoreNs = metricsRegistry.newGauge(group, "restore-ns", 0L);
    this.filesRestored = metricsRegistry.newGauge(group, "files-restored", 0L);
    this.bytesRestored = metricsRegistry.newGauge(group, "bytes-restored", 0L);

    this.filesYetToRestore = metricsRegistry.newGauge(group, "files-yet-to-restore", 0L);
    this.bytesYetToRestore = metricsRegistry.newGauge(group, "bytes-yet-to-restore", 0L);

    this.downloadMbps = metricsRegistry.newCounter(group, "download-mbps");

    this.storeRestoreNs = new ConcurrentHashMap<>();

    this.avgFileRestoreNs = metricsRegistry.newTimer(group, "avg-file-restore-ns");
  }

  public void initStoreMetrics(Collection<String> storeNames) {
    for (String storeName: storeNames) {
      storeRestoreNs.putIfAbsent(storeName,
          metricsRegistry.newGauge(group, String.format("%s-restore-ns", storeName), 0L));
    }
  }
}
