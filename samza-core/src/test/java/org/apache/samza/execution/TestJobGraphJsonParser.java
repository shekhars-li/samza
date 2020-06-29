package org.apache.samza.execution;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.StreamSpec;
import org.junit.Assert;
import org.junit.Test;


public class TestJobGraphJsonParser {

  private final static String TEST_APP_EXECUTION_PLAN_JSON =
      "{\"jobs\":[{\"jobName\":\"test-app\",\"jobId\":\"i001\",\"operatorGraph\":{\"inputStreams\":["
          + "{\"streamId\":\"test-input-topic\",\"nextOperatorIds\":[\"test-app-i001-map-1\"]}],\"outputStreams\":"
          + "[{\"streamId\":\"test-output-topic\",\"nextOperatorIds\":[]}],\"operators\":{}}}],\"sourceStreams\":"
          + "{\"test-input-topic\":{\"streamSpec\":{\"id\":\"test-input-topic\",\"systemName\":\"dev\","
          + "\"physicalName\":\"test-input-topic\",\"partitionCount\":3},\"sourceJobs\":[],\"targetJobs\":"
          + "[\"test-app\"]}},\"sinkStreams\":{\"test-output-topic\":{\"streamSpec\":{\"id\":\"test-output-topic\","
          + "\"systemName\":\"dev\",\"physicalName\":\"test-output-topic\",\"partitionCount\":1},\"sourceJobs\":"
          + "[\"test-app\"],\"targetJobs\":[]}},\"tables\":{},\"applicationName\":\"test-app\",\"applicationId\":\"i001\"}";

  private final static String TEST_TABLE_APP_EXECUTION_PLAN_JSON = "{\"jobs\":[{\"jobName\":\"test-app\",\"jobId\":"
      + "\"i001\",\"operatorGraph\":{\"inputStreams\":[{\"streamId\":\"test-input-topic\",\"nextOperatorIds\":"
      + "[\"test-app-i001-map-1\"]}],\"outputStreams\":[],\"operators\":{\"test-app-i001-join-1\":{\"opId\":"
      + "\"test-app-i001-join-1\",\"tableId\":\"test-input-table\",\"opCode\":\"JOIN\",\"sourceLocation\":\"\","
      + "\"nextOperatorIds\":[]},\"test-app-i001-send_to-2\":{\"opId\":\"test-app-i001-send_to-2\",\"tableId\":"
      + "\"test-output-table\",\"opCode\":\"SEND_TO\",\"sourceLocation\":\"\",\"nextOperatorIds\":[]}}}}],"
      + "\"sourceStreams\":{\"test-input-topic\":{\"streamSpec\":{\"id\":\"test-input-topic\",\"systemName\":\"dev\","
      + "\"physicalName\":\"test-input-topic\",\"partitionCount\":3},\"sourceJobs\":[],\"targetJobs\":[\"test-app\"]}},"
      + "\"sinkStreams\":{},\"tables\":{\"test-input-table\":{\"id\":\"test-input-table\",\"providerFactory\":"
      + "\"org.apache.samza.table.remote.RemoteTableProviderFactory\"},\"test-output-table\":{\"id\":\"test-output-table\""
      + ",\"providerFactory\":\"org.apache.samza.table.remote.RemoteTableProviderFactory\"}},\"applicationName\":"
      + "\"test-app\",\"applicationId\":\"i001\"}";

  private final static Config TEST_APP_CONFIG = new MapConfig(
      ImmutableMap.of(JobNodeConfigurationGenerator.CONFIG_INTERNAL_EXECUTION_PLAN, TEST_APP_EXECUTION_PLAN_JSON));
  private final static Config TEST_TABLE_APP_CONFIG = new MapConfig(
      ImmutableMap.of(JobNodeConfigurationGenerator.CONFIG_INTERNAL_EXECUTION_PLAN,
          TEST_TABLE_APP_EXECUTION_PLAN_JSON));

  private final static JobGraphJsonParser JOB_GRAPH_JSON_PARSER = new JobGraphJsonParser(TEST_APP_CONFIG);
  private final static JobGraphJsonParser TABLE_JOB_GRAPH_JSON_PARSER = new JobGraphJsonParser(TEST_TABLE_APP_CONFIG);

  @Test
  public void testGetSourceStreams() {
    List<StreamSpec> sourceStreams = JOB_GRAPH_JSON_PARSER.getSourceStreams();
    Assert.assertEquals(1, sourceStreams.size());
    Assert.assertEquals("test-input-topic", sourceStreams.get(0).getId());
    Assert.assertEquals("dev", sourceStreams.get(0).getSystemName());
    Assert.assertEquals("test-input-topic", sourceStreams.get(0).getPhysicalName());
    Assert.assertEquals(3, sourceStreams.get(0).getPartitionCount());
  }

  @Test
  public void testGetSinkStreams() {
    List<StreamSpec> sinkStreams = JOB_GRAPH_JSON_PARSER.getSinkStreams();
    Assert.assertEquals(1, sinkStreams.size());
    Assert.assertEquals("test-output-topic", sinkStreams.get(0).getId());
    Assert.assertEquals("dev", sinkStreams.get(0).getSystemName());
    Assert.assertEquals("test-output-topic", sinkStreams.get(0).getPhysicalName());
    Assert.assertEquals(1, sinkStreams.get(0).getPartitionCount());
  }

  @Test
  public void testGetSourceTableIdsWhenNoTableSystem() {
    List<String> sourceTableIds = JOB_GRAPH_JSON_PARSER.getSourceTableIds();
    Assert.assertTrue(sourceTableIds.isEmpty());
  }

  @Test
  public void testGetSinkTableIdsWhenNoTableSystem() {
    List<String> sinkTableIds = JOB_GRAPH_JSON_PARSER.getSinkTableIds();
    Assert.assertTrue(sinkTableIds.isEmpty());
  }

  @Test
  public void testGetSourceTableIds() {
    List<String> sourceTableIds = TABLE_JOB_GRAPH_JSON_PARSER.getSourceTableIds();
    Assert.assertEquals(1, sourceTableIds.size());
    Assert.assertEquals("test-input-table", sourceTableIds.get(0));
  }

  @Test
  public void testGetSinkTableIds() {
    List<String> sinkTableIds = TABLE_JOB_GRAPH_JSON_PARSER.getSinkTableIds();
    Assert.assertEquals(1, sinkTableIds.size());
    Assert.assertEquals("test-output-table", sinkTableIds.get(0));
  }
}
