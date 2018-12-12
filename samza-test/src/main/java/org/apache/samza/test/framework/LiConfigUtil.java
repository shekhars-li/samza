package org.apache.samza.test.framework;

import com.google.common.collect.ImmutableMap;
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
}
