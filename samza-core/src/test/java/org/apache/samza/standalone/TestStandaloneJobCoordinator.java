package org.apache.samza.standalone;

import org.apache.samza.SamzaException;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.JobModelManager;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class TestStandaloneJobCoordinator {
  public static final String MOCK_BEHAVIOR_FORMAT_STRING = "mock.%s.canSupportLoadBalancedConsumer";

  @Test(expected = SamzaException.class)
  public void testJCthrowsExceptionOnNonLBConsumer() {
    Map<String, String> partialJobConfig = new HashMap<String, String>() {
      {
        put("systems.A.samza.factory", "org.apache.samza.standalone.mock.MockSystemFactory");
        put("systems.B.samza.factory", "org.apache.samza.standalone.mock.MockSystemFactory");
        put("task.inputs", "A.topic1");
        put(String.format(MOCK_BEHAVIOR_FORMAT_STRING, "A"), "false");
        put(String.format(MOCK_BEHAVIOR_FORMAT_STRING, "B"), "true");
      }
    };
    new StandaloneJobCoordinator(1, new MapConfig(partialJobConfig), mock(JobModelManager.class));
  }

  @Test
  public void testStandaloneJCAllowsLBConsumer() {
    Map<String, String> partialJobConfig = new HashMap<String, String>() {
      {
        put("systems.A.samza.factory", "org.apache.samza.standalone.mock.MockSystemFactory");
        put("systems.B.samza.factory", "org.apache.samza.standalone.mock.MockSystemFactory");
        put("task.inputs", "A.topic1");
        put(String.format(MOCK_BEHAVIOR_FORMAT_STRING, "A"), "true");
        put(String.format(MOCK_BEHAVIOR_FORMAT_STRING, "B"), "true");
      }
    };
    new StandaloneJobCoordinator(1, new MapConfig(partialJobConfig), mock(JobModelManager.class));
  }
}
