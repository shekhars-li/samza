package org.apache.samza.standalone.mock;

import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.standalone.TestStandaloneJobCoordinator;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockSystemFactory implements SystemFactory {
  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    boolean mockShouldSupportLoadBalancedConsumer = Boolean.valueOf(
        config.get(String.format(TestStandaloneJobCoordinator.MOCK_BEHAVIOR_FORMAT_STRING, systemName), "false")
    );
    SystemConsumer mockConsumer = mock(SystemConsumer.class);
    when(mockConsumer.canSupportLoadBalancedConsumer()).thenReturn(mockShouldSupportLoadBalancedConsumer);
    return mockConsumer;
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    return null;
  }

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return null;
  }
}
