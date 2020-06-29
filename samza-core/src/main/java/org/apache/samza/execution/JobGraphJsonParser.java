package org.apache.samza.execution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.lineage.LineageException;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.system.StreamSpec;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * Linkedin-specific: This class provides the convenient methods to help parse lineage information from Samza job
 * execution plan {@code JobNodeConfigurationGenerator.CONFIG_INTERNAL_EXECUTION_PLAN}.
 */
public class JobGraphJsonParser {

  private static final String TABLE_ID_FIELD_NAME = "tableId";
  private static final String OPERATOR_CODE_FIELD_NAME = "opCode";

  private final JobGraphJsonGenerator.JobGraphJson jobGraphJson;

  /**
   * Build {@link JobGraphJsonParser} from given Samza config.<p/>
   *
   * It is the callers' responsibility to make sure config contains Samza job execution plan:
   * {@code JobNodeConfigurationGenerator.CONFIG_INTERNAL_EXECUTION_PLAN}.
   *
   * @param config Samza config
   */
  public JobGraphJsonParser(Config config) {
    String plainJson = config.get(JobNodeConfigurationGenerator.CONFIG_INTERNAL_EXECUTION_PLAN, "{}");
    ObjectMapper mapper = new ObjectMapper();
    try {
      this.jobGraphJson = mapper.readValue(plainJson.getBytes(), JobGraphJsonGenerator.JobGraphJson.class);
    } catch (IOException e) {
      throw new LineageException(String.format("Failed to deserialize raw JobGraph json: %s", plainJson), e);
    }
  }

  /**
   * Parse all source streams from {@link JobGraphJsonGenerator.JobGraphJson}.
   * @return list of {@link StreamSpec}
   */
  public List<StreamSpec> getSourceStreams() {
    return getStreams(this.jobGraphJson.sourceStreams);
  }

  /**
   * Parse all sink streams from {@link JobGraphJsonGenerator.JobGraphJson}.
   * @return list of {@link StreamSpec}
   */
  public List<StreamSpec> getSinkStreams() {
    return getStreams(this.jobGraphJson.sinkStreams);
  }

  private List<StreamSpec> getStreams(Map<String, JobGraphJsonGenerator.StreamEdgeJson> streamEdgeJsonMap) {
    List<StreamSpec> streams = new ArrayList<>();
    if (streamEdgeJsonMap == null) {
      return streams;
    }
    streamEdgeJsonMap.forEach((k, v) -> {
      if (v.streamSpec != null) {
        JobGraphJsonGenerator.StreamSpecJson streamSpec = v.streamSpec;
        streams.add(
            new StreamSpec(streamSpec.id, streamSpec.physicalName, streamSpec.systemName, streamSpec.partitionCount));
      }
    });
    return streams;
  }

  /**
   * Parse all source table ids from {@link JobGraphJsonGenerator.OperatorGraphJson}.<p/>
   *
   * Currently there is no explict information to show which table is used as source dataset, however there is op code
   * and table id mapping defined in {@link JobGraphJsonGenerator.OperatorGraphJson}.
   *
   * When Samza uses table as input system, usually it is only used in table join scenario. So the tables that
   * are using JOIN op code can be parsed as source system.
   *
   * @return input table ids
   */
  public List<String> getSourceTableIds() {
    return getTableIds(OperatorSpec.OpCode.JOIN);
  }

  /**
   * Parse all sink table ids from {@link JobGraphJsonGenerator.OperatorGraphJson}.<p/>
   *
   * Currently there is no explict information to show which table is used as sink dataset, however there is op code
   * and table id mapping defined in {@link JobGraphJsonGenerator.OperatorGraphJson}.
   *
   * When Samza uses table as output system, the table will have SEND_TO op code in its operator definition. So the
   * tables that are using SEND_TO op code can be parsed as sink system.
   *
   * @return output table ids
   */
  public List<String> getSinkTableIds() {
    return getTableIds(OperatorSpec.OpCode.SEND_TO);
  }

  private List<String> getTableIds(OperatorSpec.OpCode opCode) {
    List<String> tableIds = new ArrayList<>();
    if (jobGraphJson.jobs == null) {
      return tableIds;
    }
    List<JobGraphJsonGenerator.JobNodeJson> jobs = jobGraphJson.jobs;
    for (JobGraphJsonGenerator.JobNodeJson job : jobs) {
      if (job.operatorGraph == null || job.operatorGraph.operators == null) {
        continue;
      }
      Map<String, Map<String, Object>> operators = job.operatorGraph.operators;
      for (String opId : operators.keySet()) {
        Map<String, Object> map = operators.get(opId);
        if (map.containsKey(TABLE_ID_FIELD_NAME) && opCode.name().equals(map.get(OPERATOR_CODE_FIELD_NAME))) {
          // The value for tableId is populated from {@code JobGraphJsonGenerator.operatorToMap} method, it is always
          // String type.
          tableIds.add(String.valueOf(map.get(TABLE_ID_FIELD_NAME)));
        }
      }
    }
    return tableIds;
  }
}
