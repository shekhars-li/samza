package org.apache.samza.container.grouper.task;

import org.apache.samza.config.ConfigException;
import org.apache.samza.config.MapConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class TestSingleContainerGrouperFactory {
  private static final String PROCESSOR_ID = "processor.id";

  @Test(expected = ConfigException.class)
  public void testBuildThrowsExceptionOnMissingProcessorId() {
    SingleContainerGrouperFactory factory = new SingleContainerGrouperFactory();
    factory.build(new MapConfig());
  }

  @Test(expected = NumberFormatException.class)
  public void testBuildThrowsExceptionOnInvalidProcessorId() {
    SingleContainerGrouperFactory factory = new SingleContainerGrouperFactory();
    factory.build(new MapConfig(Collections.singletonMap(PROCESSOR_ID, "abc123")));
  }

  @Test
  public void testBuildSucceeds() {
    SingleContainerGrouperFactory factory = new SingleContainerGrouperFactory();
    TaskNameGrouper grouper = factory.build(new MapConfig(Collections.singletonMap(PROCESSOR_ID, "1")));
    Assert.assertNotNull(grouper);
    Assert.assertTrue(grouper instanceof SingleContainerGrouper);
  }
}
