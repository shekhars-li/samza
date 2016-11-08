package org.apache.samza.container.grouper.stream;

import junit.framework.Assert;
import org.apache.samza.Partition;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TestAllSspToSingleTaskGrouper {
  private static final String PROCESSOR_ID = "processor.id";

  @Test
  public void testGrouperCreatesTasknameWithProcessorId() {
    Map<String, String> configMap = new HashMap<String, String>() {
      {
        put(PROCESSOR_ID, "1234");
      }
    };

    Set<SystemStreamPartition> sspSet = new HashSet<SystemStreamPartition>() {
      {
        add(new SystemStreamPartition("test-system", "test-stream-1", new Partition(0)));
        add(new SystemStreamPartition("test-system", "test-stream-1", new Partition(1)));
        add(new SystemStreamPartition("test-system", "test-stream-2", new Partition(0)));
        add(new SystemStreamPartition("test-system", "test-stream-2", new Partition(1)));
      }
    };
    SystemStreamPartitionGrouper sspGrouper =
        new AllSspToSingleTaskGrouperFactory().getSystemStreamPartitionGrouper(new MapConfig(configMap));

    Map<TaskName, Set<SystemStreamPartition>> result = sspGrouper.group(sspSet);
    Assert.assertNotNull(result);
    Assert.assertEquals(1, result.size());

    TaskName taskName = (TaskName) result.keySet().toArray()[0];
    Assert.assertTrue(
        "Task & container is synonymous in this grouper. Cannot find processor ID in the TaskName!",
        taskName.getTaskName().contains("1234"));
  }

}
