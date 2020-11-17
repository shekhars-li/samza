package org.apache.samza.test.framework;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Map;


/**
 * Linkedin-specific utilities for config generation for tests.
 */
public class LiConfigUtil {
  /**
   * Build Linkedin-specific required configs for default Offspring components.
   */
  public static Map<String, String> buildRequiredOffspringConfigs(String appName) {
    return ImmutableMap.of(
      "serviceCallHelper.notificationType", "log",
      "serviceCallHelper.notificationLogRate", "1m",
      "com.linkedin.app.env", "dev",
      "com.linkedin.app.name", appName,
      "com.linkedin.app.instance", "i001");
  }

  public static Map<String, String> buildRequiredTestConfigs(String appName) {
    HashMap<String, String> samzaTestConfigs = Maps.newHashMap();

    /*
     * Required configs for default Offspring components.
     */
    samzaTestConfigs.putAll(buildRequiredOffspringConfigs(appName));

    /*
     * Build Linkedin-specific required avro SchemaRegistry configs for jobs to use ei-ltx1 schema registry dns discovery
     * for serializing / deserializing avro specific and generic records, also increase schema registry timeout since in
     * PCX tests run on not just ltx but lva, lca colos and cross-colo schema registry latency increases
     */
    samzaTestConfigs.putAll(ImmutableMap.of(
        "serializers.registry.avro.schemas", "http://1.schemaregistry.ei-ltx1.atd.stg.linkedin.com:10252/schemaRegistry/schemas",
        "serializers.registry.avro.connection.timeout.ms", "60000"));

    /*
     * Disable JMX for TestRunner tests by default
     */
    samzaTestConfigs.put("job.jmx.enabled", "false");

    return samzaTestConfigs;
  }


}
