package org.apache.samza.container;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSet;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigRewriter;
import org.apache.samza.config.JobConfig;
import org.apache.samza.container.grouper.task.TaskAssignmentManager;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.coordinator.stream.messages.SetTaskContainerMapping;
import org.apache.samza.coordinator.stream.messages.SetTaskModeMapping;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.util.CommandLine;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.apache.samza.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LocalityTool extends CommandLine {
  private static final Logger LOG = LoggerFactory.getLogger(LocalityTool.class);
  private static final String DELETE_VALUE = "null";

  private final LocalityManager localityManager;
  private final TaskAssignmentManager taskAssignmentManager;

  /**
   * Main function for using the LocalityTool. The main function creates a locality manager
   * and either prints to the LOG if 0 or 1 arguments are provided, or sets the value if 2 are provided.
   *
   * To run the code use the following command:
   *  {deployment dir}/bin/run-locality-tool.sh  --config-factory={factory} --config-path={path} --containerId={[optional]container} --host={[optional] preferred host for the container}
   *
   * Examples
   *  Print all locality:                     ./bin/run-locality-tool.sh  --config-factory={factory} --config-path={path}
   *
   *  Print preferred host for a container:   ./bin/run-locality-tool.sh  --config-factory={factory} --config-path={path} --containerId=2
   *  Assign preferred host for a container:  ./bin/run-locality-tool.sh  --config-factory={factory} --config-path={path} --containerId=2 --host=x.y.z
   *  Delete preferred host for a container:  ./bin/run-locality-tool.sh  --config-factory={factory} --config-path={path} --containerId=2 --host=null
   *
   *  Print container for a task:             ./bin/run-locality-tool.sh  --config-factory={factory} --config-path={path} --taskName="Partition 26"
   *  Assign container for a task:            ./bin/run-locality-tool.sh  --config-factory={factory} --config-path={path} --taskName="Partition 26" --containerId=2
   *  Delete container for a task:            ./bin/run-locality-tool.sh  --config-factory={factory} --config-path={path} --taskName="Partition 26" --containerId=null
   *
   * @param args input arguments for running the tool. These arguments are:
   *             "config-factory" The config file factory
   *             "config-path"    The path to config file of a job
   *             "containerId"    [optional] the containerId to write as key or value
   *             "taskName"       [optional] the taskName to write as key or value (null value == delete)
   *             "host"           [optional] the host to write as value (null value == delete)
   */
  public static void main(String[] args) {
    LocalityToolCommandLine commandLine = new LocalityToolCommandLine();
    OptionSet options = commandLine.parser().parse(args);
    LocalityTool lt = new LocalityTool(commandLine.loadConfig(options));

    try {
      String containerId = commandLine.getContainerId(options);
      String taskName = commandLine.getTaskName(options);
      String host = commandLine.getHost(options);

      if (taskName != null && host != null) {
        throw new RuntimeException("Got values for both taskName and host. For safety, you can only set a container->host or a task->container mapping, not both.");
      }

      if (containerId != null) {
        if (host != null) {
          lt.setContainerLocality(containerId, host);
        } else if (taskName != null) {
          lt.setTaskLocality(taskName, containerId);
        } else {
          lt.printContainerLocality(containerId);
        }
      } else if (taskName != null) {
        lt.printTaskLocality(taskName);
      } else {
        lt.printAllContainerLocality();
        lt.printAllTaskLocality();
      }

    } catch (Throwable t) {
      lt.close();
      throw t;
    }
  }

  public LocalityTool(Config config) {
    Config rconfig = rewriteJobConfig(new JobConfig(config));
    MetricsRegistry metricsRegistry = new NoOpMetricsRegistry();

    CoordinatorStreamStore coordinatorStreamStore = new CoordinatorStreamStore(rconfig, metricsRegistry);
    this.localityManager = new LocalityManager(coordinatorStreamStore);
    this.taskAssignmentManager = new TaskAssignmentManager(
        new NamespaceAwareCoordinatorStreamStore(coordinatorStreamStore, SetTaskContainerMapping.TYPE),
        new NamespaceAwareCoordinatorStreamStore(coordinatorStreamStore, SetTaskModeMapping.TYPE));
  }

  private Config rewriteJobConfig(JobConfig jobConfig) {
    Config config = jobConfig;
    if (jobConfig.getConfigRewriters().isPresent()) {
      for (String rewriterName : jobConfig.getConfigRewriters().get().split(",")) {
        config = rewrite(jobConfig, config, rewriterName);
      }
    }
    return config;
  }

  private Config rewrite(JobConfig jobConfig, Config config, String rewriterName) {
    String rewriterClassName = jobConfig
        .getConfigRewriterClass(rewriterName)
        .orElseThrow(() -> new SamzaException(
            String.format("Unable to find class config for config rewriter %s.", rewriterName)));

    ConfigRewriter rewriter = ReflectionUtil.getObj(rewriterClassName, ConfigRewriter.class);
    LOG.info("Re-writing config for CheckpointTool with " + rewriter);
    return rewriter.rewrite(rewriterName, config);
  }

  public void close() {
    localityManager.close();
    taskAssignmentManager.close();
  }

  public void printAllContainerLocality() {
    LOG.info("Container Locality:");
    for (Map.Entry<String, Map<String, String>> entry : localityManager.readContainerLocality().entrySet()) {
      LOG.info("--containerId={} --host={}", entry.getKey(), entry.getValue().get(SetContainerHostMapping.HOST_KEY));
    }
  }

  public void printContainerLocality(String containerId) {
    Map<String, String> containerLocality = localityManager.readContainerLocality().get(containerId);
    LOG.info("Container Locality:");
    if (containerLocality != null) {
      LOG.info("--containerId={} --host={}", containerId, containerLocality.get(SetContainerHostMapping.HOST_KEY));
    } else {
      LOG.info("Container \"{}\" has no preferred host", containerId);
    }
  }

  public void setContainerLocality(String containerId, String host) {
    LOG.info("Writing container locality for container \"{}\" to host \"{}\".", containerId, host);
    if (DELETE_VALUE.equalsIgnoreCase(host)) {
      localityManager.deleteContainerLocality(containerId);
    } else {
      localityManager.writeContainerToHostMapping(containerId, host);
    }
    LOG.info("Container locality written successfully.");
  }

  public void printAllTaskLocality() {
    LOG.info("Task Locality:");
    for (Map.Entry<String, String> entry : taskAssignmentManager.readTaskAssignment().entrySet()) {
      LOG.info("--taskName=\"{}\" --containerId={}", entry.getKey(), entry.getValue());
    }

  }

  public void printTaskLocality(String taskName) {
    String taskAssignment = taskAssignmentManager.readTaskAssignment().get(taskName);
    LOG.info("Task Locality:");
    if (taskAssignment != null) {
      LOG.info("--taskName=\"{}\" --containerId={}", taskName, taskAssignment);
    } else {
      LOG.info("Task \"{}\" has no assigned container", taskName);
    }
  }

  public void setTaskLocality(String taskName, String containerId) {
    LOG.info("Writing task locality for task \"{}\" to container \"{}\".", taskName, containerId);
    if (DELETE_VALUE.equalsIgnoreCase(containerId)) {
      taskAssignmentManager.deleteTaskContainerMappings(ImmutableList.of(taskName));
    } else {
      taskAssignmentManager.writeTaskContainerMappings(ImmutableMap.of(containerId, ImmutableMap.of(taskName, TaskMode.Active)));
    }
    LOG.info("Task locality written successfully.");
  }

  public static class LocalityToolCommandLine extends CommandLine {
    private final ArgumentAcceptingOptionSpec<String>
        containerIdArg = parser().acceptsAll(ImmutableList.of("c", "containerId", "container"),
        "Container Id to write as key or value. If provided alone it prints the current preferred host. If provided with a host, the preferred host will be written.")
        .withRequiredArg().ofType(String.class).describedAs("container");
    private final ArgumentAcceptingOptionSpec<String>
        taskNameArg = parser().acceptsAll(ImmutableList.of("t", "taskName", "task"),
        "Task name to write as key. If provided alone it prints the current container assignment. If provided with containerId, the container assignment will be written.")
        .withRequiredArg().ofType(String.class).describedAs("task");
    private final ArgumentAcceptingOptionSpec<String>
        hostArg = parser().acceptsAll(ImmutableList.of("h", "hostName", "host"),
        "Host to write as value").withRequiredArg().ofType(String.class).describedAs("host");

    public String getContainerId(OptionSet options) {
      return options.valueOf(containerIdArg);
    }

    public String getTaskName(OptionSet options) {
      return options.valueOf(taskNameArg);
    }

    public String getHost(OptionSet options) {
      return options.valueOf(hostArg);
    }
  }
}
