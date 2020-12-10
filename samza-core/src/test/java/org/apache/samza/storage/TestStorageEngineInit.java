package org.apache.samza.storage;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import org.apache.samza.Partition;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.container.TaskInstance;
import org.apache.samza.container.TaskInstanceMetrics;
import org.apache.samza.container.TaskName;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerdeFactory;
import org.apache.samza.system.SSPMetadataCache;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.SystemClock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;


public class TestStorageEngineInit {

  private static final String STORE_NAME = "store";
  private static final String SYSTEM_NAME = "kafka";
  private static final File DEFAULT_STORE_BASE_DIR = new File(System.getProperty("java.io.tmpdir") + File.separator + "store");
  private static final File
      DEFAULT_LOGGED_STORE_BASE_DIR = new File(System.getProperty("java.io.tmpdir") + File.separator + "loggedStore");

  private ContainerStorageManager containerStorageManager;
  private Map<TaskName, Gauge<Object>> taskInitMetricGauges;
  private Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics;
  private SamzaContainerMetrics samzaContainerMetrics;
  private Map<TaskName, TaskModel> tasks;

  private volatile int storeInitCallCount;

  /**
   * Utility method for creating a mocked taskInstance and taskStorageManager and adding it to the map.
   * @param taskname the desired taskname.
   */
  private void addMockedTask(String taskname, int changelogPartition) {
    TaskInstance mockTaskInstance = mock(TaskInstance.class);
    doAnswer(invocation -> {
      return new TaskName(taskname);
    }).when(mockTaskInstance).taskName();

    Gauge testGauge = mock(Gauge.class);
    this.tasks.put(new TaskName(taskname),
        new TaskModel(new TaskName(taskname), new HashSet<>(), new Partition(changelogPartition)));
    this.taskInitMetricGauges.put(new TaskName(taskname), testGauge);
    this.taskInstanceMetrics.put(new TaskName(taskname), mock(TaskInstanceMetrics.class));
  }

  /**
   * Method to create a containerStorageManager with mocked dependencies
   */
  @Before
  public void setUp() throws InterruptedException {
    taskInitMetricGauges = new HashMap<>();
    this.tasks = new HashMap<>();
    this.taskInstanceMetrics = new HashMap<>();

    // Add two mocked tasks
    addMockedTask("task 0", 0);
    addMockedTask("task 1", 1);

    // Mock container metrics
    samzaContainerMetrics = mock(SamzaContainerMetrics.class);
    when(samzaContainerMetrics.taskStoreInitMetrics()).thenReturn(taskInitMetricGauges);

    // Changelog stream ssp must be empty since for stores like DaVinci we do not control it
    Map<String, SystemStream> changelogSystemStreams = new HashMap<>();

    // Create mocked storage engine factories
    Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories = new HashMap<>();
    StorageEngineFactory mockStorageEngineFactory =
        (StorageEngineFactory<Object, Object>) mock(StorageEngineFactory.class);
    StorageEngine mockStorageEngine = mock(StorageEngine.class);
    when(mockStorageEngine.getStoreProperties())
        .thenReturn(new StoreProperties.StorePropertiesBuilder().setLoggedStore(false).setPersistedToDisk(false).build());
    doAnswer(invocation -> {
      return mockStorageEngine;
    }).when(mockStorageEngineFactory).getStorageEngine(anyString(), any(), any(), any(), any(),
        any(), any(), any(), any(), any(), any());

    storageEngineFactories.put(STORE_NAME, mockStorageEngineFactory);

    // Add instrumentation to mocked storage engine, to record the number of store.init() calls
    doAnswer(invocation -> {
      storeInitCallCount++;
      return null;
    }).when(mockStorageEngine).init(any());

    // Set the mocked stores' properties to be persistent
    doAnswer(invocation -> {
      return new StoreProperties.StorePropertiesBuilder().setLoggedStore(false).build();
    }).when(mockStorageEngine).getStoreProperties();

    // Mock and setup sysconsumers
    SystemConsumer mockSystemConsumer = mock(SystemConsumer.class);

    // Create mocked system factories
    Map<String, SystemFactory> systemFactories = new HashMap<>();

    // Count the number of sysConsumers created
    SystemFactory mockSystemFactory = mock(SystemFactory.class);

    systemFactories.put(SYSTEM_NAME, mockSystemFactory);

    // Create mocked configs for specifying serdes
    Map<String, String> configMap = new HashMap<>();
    configMap.put("stores." + STORE_NAME + ".key.serde", "stringserde");
    configMap.put("stores." + STORE_NAME + ".msg.serde", "stringserde");
    configMap.put("serializers.registry.stringserde.class", StringSerdeFactory.class.getName());
    Config config = new MapConfig(configMap);

    Map<String, Serde<Object>> serdes = new HashMap<>();
    serdes.put("stringserde", mock(Serde.class));

    // Create mocked system admins
    SystemAdmin mockSystemAdmin = mock(SystemAdmin.class);
    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        System.out.println("called with arguments: " + Arrays.toString(args));
        return null;
      }
    }).when(mockSystemAdmin).validateStream(any());
    SystemAdmins mockSystemAdmins = mock(SystemAdmins.class);
    when(mockSystemAdmins.getSystemAdmin("kafka")).thenReturn(mockSystemAdmin);

    // Create a mocked mockStreamMetadataCache

    CheckpointManager checkpointManager = mock(CheckpointManager.class);
    when(checkpointManager.readLastCheckpoint(any(TaskName.class))).thenReturn(new Checkpoint(new HashMap<>()));

    SSPMetadataCache mockSSPMetadataCache = mock(SSPMetadataCache.class);
    when(mockSSPMetadataCache.getMetadata(any(SystemStreamPartition.class)))
        .thenReturn(new SystemStreamMetadata.SystemStreamPartitionMetadata("0", "10", "11"));

    // Reset the  expected number of sysConsumer create, start and stop calls, and store.restore() calls
    this.storeInitCallCount = 0;

    // Create the container storage manager
    this.containerStorageManager = new ContainerStorageManager(
        checkpointManager,
        new ContainerModel("samza-container-test", tasks),
        mock(StreamMetadataCache.class),
        mockSSPMetadataCache,
        mockSystemAdmins,
        changelogSystemStreams,
        new HashMap<>(),
        storageEngineFactories,
        systemFactories,
        serdes,
        config,
        taskInstanceMetrics,
        samzaContainerMetrics,
        mock(JobContext.class),
        mock(ContainerContext.class),
        Optional.of(mock(ExternalContext.class)),
        mock(Map.class),
        DEFAULT_LOGGED_STORE_BASE_DIR,
        DEFAULT_STORE_BASE_DIR,
        2,
        null,
        new SystemClock());
  }

  @Test
  public void testInitParallelismAndMetrics() throws InterruptedException {
    this.containerStorageManager.start();
    this.containerStorageManager.shutdown();

    for (Gauge gauge : taskInitMetricGauges.values()) {
      Assert.assertTrue("Init time gauge value should be invoked atleast once",
          mockingDetails(gauge).getInvocations().size() >= 1);
    }

    Assert.assertTrue("Store init count should be 2 because there are 2 tasks", this.storeInitCallCount == 2);

  }
}

