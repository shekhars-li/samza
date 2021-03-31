/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.storage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.config.Config;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.RunLoop;
import org.apache.samza.container.RunLoopTask;
import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.container.TaskInstanceMetrics;
import org.apache.samza.container.TaskName;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.Context;
import org.apache.samza.context.ContextImpl;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeManager;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SSPMetadataCache;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemConsumers;
import org.apache.samza.system.SystemConsumersMetrics;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.chooser.DefaultChooser;
import org.apache.samza.system.chooser.MessageChooser;
import org.apache.samza.system.chooser.RoundRobinChooserFactory;
import org.apache.samza.table.utils.SerdeUtils;
import org.apache.samza.task.TaskInstanceCollector;
import org.apache.samza.util.Clock;
import org.apache.samza.util.ReflectionUtil;
import org.apache.samza.util.ScalaJavaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConversions;


/**
 *  ContainerStorageManager is a per-container object that manages
 *  the restore of per-task partitions.
 *
 *  It is responsible for
 *  a) performing all container-level actions for restore such as, initializing and shutting down
 *  taskStorage managers, starting, registering and stopping consumers, etc.
 *
 *  b) performing individual task stores' restores in parallel.
 *
 *  and
 *  c) restoring sideInputs.
 *  It provides bootstrap semantics for sideinputs -- the main thread is blocked until
 *  all sideInputSSPs have not caught up. Side input store flushes are not in sync with task-commit, although
 *  they happen at the same frequency.
 *  In case, where a user explicitly requests a task-commit, it will not include committing sideInputs.
 */
public class ContainerStorageManager {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerStorageManager.class);
  private static final String DAVINCI_KV_STORAGE_ENGINE_FACTORY =
      "com.linkedin.samza.kv.davinci.DaVinciStorageEngineFactory";

  private static final String RESTORE_THREAD_NAME = "Samza Restore Thread-%d";
  private static final String STORE_INIT_THREAD_NAME = "Samza Store Init Thread-%d";
  private static final String SIDEINPUTS_THREAD_NAME = "SideInputs Thread";
  private static final String SIDEINPUTS_METRICS_PREFIX = "side-inputs-";
  // We use a prefix to differentiate the SystemConsumersMetrics for sideInputs from the ones in SamzaContainer

  // Timeout with which sideinput thread checks for exceptions and for whether SSPs as caught up
  private static final int SIDE_INPUT_CHECK_TIMEOUT_SECONDS = 10;
  private static final int SIDE_INPUT_SHUTDOWN_TIMEOUT_SECONDS = 60;

  /** Maps containing relevant per-task objects */
  private final Map<TaskName, Map<String, StorageEngine>> taskStores;
  private final Map<TaskName, TaskRestoreManager> taskRestoreManagers;
  private final Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics;
  private final Map<TaskName, TaskInstanceCollector> taskInstanceCollectors;

  private final Map<String, SystemConsumer> storeConsumers; // Mapping from store name to SystemConsumers
  private final Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories; // Map of storageEngineFactories indexed by store name
  private final Map<String, SystemStream> changelogSystemStreams; // Map of changelog system-streams indexed by store name
  private final Map<String, Serde<Object>> serdes; // Map of Serde objects indexed by serde name (specified in config)
  private final SystemAdmins systemAdmins;

  private final StreamMetadataCache streamMetadataCache;
  private final SSPMetadataCache sspMetadataCache;
  private final SamzaContainerMetrics samzaContainerMetrics;

  private final CheckpointManager checkpointManager;
  /* Parameters required to re-create taskStores post-restoration */
  private final ContainerModel containerModel;
  private final JobContext jobContext;
  private final ContainerContext containerContext;
  private final Optional<ExternalContext> externalContextOptional;
  private final Context context;

  private final File loggedStoreBaseDirectory;
  private final File nonLoggedStoreBaseDirectory;
  private final Set<Path> storeDirectoryPaths; // the set of store directory paths, used by SamzaContainer to initialize its disk-space-monitor

  private final int parallelRestoreThreadPoolSize;
  private final int parallelInitThreadPoolSize;
  private final int maxChangeLogStreamPartitions; // The partition count of each changelog-stream topic. This is used for validating changelog streams before restoring.

  /* Sideinput related parameters */
  private final boolean hasSideInputs;
  // side inputs indexed first by task, then store name
  private final Map<TaskName, Map<String, Set<SystemStreamPartition>>> taskSideInputStoreSSPs;
  private final Map<SystemStreamPartition, TaskSideInputHandler> sspSideInputHandlers;
  private SystemConsumers sideInputSystemConsumers;
  private volatile Map<TaskName, CountDownLatch> sideInputTaskLatches; // Used by the sideInput-read thread to signal to the main thread
  private volatile boolean shouldShutdown = false;
  private RunLoop sideInputRunLoop;

  private final ExecutorService sideInputsExecutor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat(SIDEINPUTS_THREAD_NAME).build());

  private volatile Throwable sideInputException = null;

  private final Config config;
  private final StorageManagerUtil storageManagerUtil = new StorageManagerUtil();

  public ContainerStorageManager(
      CheckpointManager checkpointManager,
      ContainerModel containerModel,
      StreamMetadataCache streamMetadataCache,
      SSPMetadataCache sspMetadataCache,
      SystemAdmins systemAdmins,
      Map<String, SystemStream> changelogSystemStreams,
      Map<String, Set<SystemStream>> sideInputSystemStreams,
      Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Map<String, SystemFactory> systemFactories,
      Map<String, Serde<Object>> serdes,
      Config config,
      Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics,
      SamzaContainerMetrics samzaContainerMetrics,
      JobContext jobContext,
      ContainerContext containerContext,
      Optional<ExternalContext> externalContextOptional,
      Map<TaskName, TaskInstanceCollector> taskInstanceCollectors,
      File loggedStoreBaseDirectory,
      File nonLoggedStoreBaseDirectory,
      int maxChangeLogStreamPartitions,
      SerdeManager serdeManager,
      Clock clock) {
    this.checkpointManager = checkpointManager;
    this.containerModel = containerModel;
    this.taskSideInputStoreSSPs = getTaskSideInputSSPs(containerModel, sideInputSystemStreams);
    this.sideInputTaskLatches = new HashMap<>();
    this.hasSideInputs = this.taskSideInputStoreSSPs.values().stream()
        .flatMap(m -> m.values().stream())
        .flatMap(Collection::stream)
        .findAny()
        .isPresent();
    this.sspMetadataCache = sspMetadataCache;
    this.changelogSystemStreams = getChangelogSystemStreams(containerModel, changelogSystemStreams); // handling standby tasks

    LOG.info("Starting with changelogSystemStreams = {} taskSideInputStoreSSPs = {}", this.changelogSystemStreams, this.taskSideInputStoreSSPs);

    this.storageEngineFactories = storageEngineFactories;
    this.serdes = serdes;
    this.loggedStoreBaseDirectory = loggedStoreBaseDirectory;
    this.nonLoggedStoreBaseDirectory = nonLoggedStoreBaseDirectory;

    if (loggedStoreBaseDirectory != null && loggedStoreBaseDirectory.equals(nonLoggedStoreBaseDirectory)) {
      LOG.warn("Logged and non-logged store base directory are configured to same path: {}. It is recommended to configure"
          + "them separately to ensure clean up of non-logged store data doesn't accidentally impact logged store data.",
          loggedStoreBaseDirectory);
    }

    // set the config
    this.config = config;

    this.taskInstanceMetrics = taskInstanceMetrics;

    // Setting the metrics registry
    this.samzaContainerMetrics = samzaContainerMetrics;

    this.jobContext = jobContext;
    this.containerContext = containerContext;

    this.externalContextOptional = externalContextOptional;

    // ContainerStorageManager gets constructed before the TaskInstance hence the taskContext is passed as empty here
    this.context = new ContextImpl(jobContext, containerContext, Optional.empty(), Optional.empty(), Optional.empty(),
        externalContextOptional);

    this.taskInstanceCollectors = taskInstanceCollectors;

    // initializing the set of store directory paths
    this.storeDirectoryPaths = new HashSet<>();

    // Setting the restore thread pool size equal to the number of taskInstances
    this.parallelRestoreThreadPoolSize = containerModel.getTasks().size();

    // Setting the init thread pool size equal to the number of taskInstances
    this.parallelInitThreadPoolSize = containerModel.getTasks().size();

    this.maxChangeLogStreamPartitions = maxChangeLogStreamPartitions;
    this.streamMetadataCache = streamMetadataCache;
    this.systemAdmins = systemAdmins;

    // create taskStores for all tasks in the containerModel and each store in storageEngineFactories
    this.taskStores = createTaskStores(containerModel, jobContext, containerContext, storageEngineFactories, serdes, taskInstanceMetrics, taskInstanceCollectors);

    Set<String> containerChangelogSystems = this.changelogSystemStreams.values().stream()
        .map(SystemStream::getSystem)
        .collect(Collectors.toSet());

    // create system consumers (1 per store system in changelogSystemStreams), and index it by storeName
    Map<String, SystemConsumer> storeSystemConsumers = createConsumers(
        containerChangelogSystems, systemFactories, config, this.samzaContainerMetrics.registry());
    this.storeConsumers = createStoreIndexedMap(this.changelogSystemStreams, storeSystemConsumers);

    // creating task restore managers
    this.taskRestoreManagers = createTaskRestoreManagers(systemAdmins, clock, this.samzaContainerMetrics);

    this.sspSideInputHandlers = createSideInputHandlers(clock);

    // create SystemConsumers for consuming from taskSideInputSSPs, if sideInputs are being used
    if (this.hasSideInputs) {
      Set<SystemStream> containerSideInputSystemStreams = this.taskSideInputStoreSSPs.values().stream()
          .flatMap(map -> map.values().stream())
          .flatMap(Set::stream)
          .map(SystemStreamPartition::getSystemStream)
          .collect(Collectors.toSet());

      Set<String> containerSideInputSystems = containerSideInputSystemStreams.stream()
          .map(SystemStream::getSystem)
          .collect(Collectors.toSet());

      // create sideInput consumers indexed by systemName
      // Mapping from storeSystemNames to SystemConsumers
      Map<String, SystemConsumer> sideInputConsumers =
          createConsumers(containerSideInputSystems, systemFactories, config, this.samzaContainerMetrics.registry());

      scala.collection.immutable.Map<SystemStream, SystemStreamMetadata> inputStreamMetadata = streamMetadataCache.getStreamMetadata(JavaConversions.asScalaSet(containerSideInputSystemStreams).toSet(), false);

      SystemConsumersMetrics sideInputSystemConsumersMetrics = new SystemConsumersMetrics(samzaContainerMetrics.registry(), SIDEINPUTS_METRICS_PREFIX);
      // we use the same registry as samza-container-metrics

      MessageChooser chooser = DefaultChooser.apply(inputStreamMetadata, new RoundRobinChooserFactory(), config,
          sideInputSystemConsumersMetrics.registry(), systemAdmins);

      sideInputSystemConsumers =
          new SystemConsumers(chooser, ScalaJavaUtil.toScalaMap(sideInputConsumers), systemAdmins, serdeManager,
              sideInputSystemConsumersMetrics, SystemConsumers.DEFAULT_NO_NEW_MESSAGES_TIMEOUT(), SystemConsumers.DEFAULT_DROP_SERIALIZATION_ERROR(),
              TaskConfig.DEFAULT_POLL_INTERVAL_MS, ScalaJavaUtil.toScalaFunction(() -> System.nanoTime()));
    }

  }

  /**
   * Add all sideInputs to a map of maps, indexed first by taskName, then by sideInput store name.
   *
   * @param containerModel the containerModel to use
   * @param sideInputSystemStreams the map of store to sideInput system stream
   * @return taskSideInputSSPs map
   */
  private Map<TaskName, Map<String, Set<SystemStreamPartition>>> getTaskSideInputSSPs(ContainerModel containerModel, Map<String, Set<SystemStream>> sideInputSystemStreams) {
    Map<TaskName, Map<String, Set<SystemStreamPartition>>> taskSideInputSSPs = new HashMap<>();

    containerModel.getTasks().forEach((taskName, taskModel) -> {
      taskSideInputSSPs.putIfAbsent(taskName, new HashMap<>());
      sideInputSystemStreams.keySet().forEach(storeName -> {
        Set<SystemStreamPartition> taskSideInputs = taskModel.getSystemStreamPartitions().stream().filter(ssp -> sideInputSystemStreams.get(storeName).contains(ssp.getSystemStream())).collect(Collectors.toSet());
        taskSideInputSSPs.get(taskName).put(storeName, taskSideInputs);
      });
    });
    return taskSideInputSSPs;
  }

  /**
   * For each standby task, we remove its changeLogSSPs from changelogSSP map and add it to the task's taskSideInputSSPs.
   * The task's sideInputManager will consume and restore these as well.
   *
   * @param containerModel the container's model
   * @param changelogSystemStreams the passed in set of changelogSystemStreams
   * @return A map of changeLogSSP to storeName across all tasks, assuming no two stores have the same changelogSSP
   */
  private Map<String, SystemStream> getChangelogSystemStreams(ContainerModel containerModel, Map<String, SystemStream> changelogSystemStreams) {

    if (MapUtils.invertMap(changelogSystemStreams).size() != changelogSystemStreams.size()) {
      throw new SamzaException("Two stores cannot have the same changelog system-stream");
    }

    Map<SystemStreamPartition, String> changelogSSPToStore = new HashMap<>();
    changelogSystemStreams.forEach((storeName, systemStream) ->
        containerModel.getTasks().forEach((taskName, taskModel) -> { changelogSSPToStore.put(new SystemStreamPartition(systemStream, taskModel.getChangelogPartition()), storeName); })
    );

    getTasks(containerModel, TaskMode.Standby).forEach((taskName, taskModel) -> {
      this.taskSideInputStoreSSPs.putIfAbsent(taskName, new HashMap<>());
      changelogSystemStreams.forEach((storeName, systemStream) -> {
        SystemStreamPartition ssp = new SystemStreamPartition(systemStream, taskModel.getChangelogPartition());
        changelogSSPToStore.remove(ssp);
        this.taskSideInputStoreSSPs.get(taskName).put(storeName, Collections.singleton(ssp));
      });
    });

    // changelogSystemStreams correspond only to active tasks (since those of standby-tasks moved to sideInputs above)
    return MapUtils.invertMap(changelogSSPToStore).entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, x -> x.getValue().getSystemStream()));
  }


  /**
   *  Creates SystemConsumer objects for store restoration, creating one consumer per system.
   */
  private static Map<String, SystemConsumer> createConsumers(Set<String> storeSystems,
      Map<String, SystemFactory> systemFactories, Config config, MetricsRegistry registry) {
    // Create one consumer for each system in use, map with one entry for each such system
    Map<String, SystemConsumer> consumers = new HashMap<>();

    // Iterate over the list of storeSystems and create one sysConsumer per system
    for (String storeSystemName : storeSystems) {
      SystemFactory systemFactory = systemFactories.get(storeSystemName);
      if (systemFactory == null) {
        throw new SamzaException("System " + storeSystemName + " does not exist in config");
      }
      consumers.put(storeSystemName, systemFactory.getConsumer(storeSystemName, config, registry));
    }

    return consumers;
  }

  private static Map<String, SystemConsumer> createStoreIndexedMap(Map<String, SystemStream> changelogSystemStreams,
      Map<String, SystemConsumer> storeSystemConsumers) {
    // Map of each storeName to its respective systemConsumer
    Map<String, SystemConsumer> storeConsumers = new HashMap<>();

    // Populate the map of storeName to its relevant systemConsumer
    for (String storeName : changelogSystemStreams.keySet()) {
      storeConsumers.put(storeName, storeSystemConsumers.get(changelogSystemStreams.get(storeName).getSystem()));
    }
    return storeConsumers;
  }

  private Map<TaskName, TaskRestoreManager> createTaskRestoreManagers(SystemAdmins systemAdmins, Clock clock, SamzaContainerMetrics samzaContainerMetrics) {
    Map<TaskName, TaskRestoreManager> taskRestoreManagers = new HashMap<>();
    // DaVinci data subscription occurs in StorageEngine#init hence restoration is skipped
    containerModel.getTasks().forEach((taskName, taskModel) -> {
      taskRestoreManagers.put(taskName,
          TaskRestoreManagerFactory.create(
              taskModel, changelogSystemStreams, getNonSideInputNonDaVinciStores(taskName), systemAdmins,
              streamMetadataCache, sspMetadataCache, storeConsumers, maxChangeLogStreamPartitions,
              loggedStoreBaseDirectory, nonLoggedStoreBaseDirectory, config, clock));
      samzaContainerMetrics.addStoresRestorationGauge(taskName);
    });
    return taskRestoreManagers;
  }

  // Helper method to filter active Tasks from the container model
  private static Map<TaskName, TaskModel> getTasks(ContainerModel containerModel, TaskMode taskMode) {
    return containerModel.getTasks().entrySet().stream()
            .filter(x -> x.getValue().getTaskMode().equals(taskMode)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Create taskStores for all stores in storageEngineFactories.
   * The store mode is chosen as bulk-load if its a non-sideinput store, and readWrite if its a sideInput store
   */
  private Map<TaskName, Map<String, StorageEngine>> createTaskStores(ContainerModel containerModel, JobContext jobContext, ContainerContext containerContext,
      Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories, Map<String, Serde<Object>> serdes,
      Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics,
      Map<TaskName, TaskInstanceCollector> taskInstanceCollectors) {

    Map<TaskName, Map<String, StorageEngine>> taskStores = new HashMap<>();

    // iterate over each task in the containerModel, and each store in storageEngineFactories
    for (Map.Entry<TaskName, TaskModel> task : containerModel.getTasks().entrySet()) {
      TaskName taskName = task.getKey();
      TaskModel taskModel = task.getValue();

      if (!taskStores.containsKey(taskName)) {
        taskStores.put(taskName, new HashMap<>());
      }

      for (String storeName : storageEngineFactories.keySet()) {

        StorageEngineFactory.StoreMode storeMode = this.taskSideInputStoreSSPs.get(taskName).containsKey(storeName) ?
            StorageEngineFactory.StoreMode.ReadWrite : StorageEngineFactory.StoreMode.BulkLoad;

        StorageEngine storageEngine =
            createStore(storeName, taskName, taskModel, jobContext, containerContext, storageEngineFactories, serdes, taskInstanceMetrics, taskInstanceCollectors, storeMode);

        // add created store to map
        taskStores.get(taskName).put(storeName, storageEngine);
        LOG.info("Created store {} for task {} in mode {}", storeName, taskName, storeMode);
      }

      // initialize the Store init time metrics
      samzaContainerMetrics.addStoresInitGauge(taskName);
    }

    return taskStores;
  }

  /**
   * Recreate all non-sideInput persistent stores in ReadWrite mode.
   *
   */
  private void recreatePersistentTaskStoresInReadWriteMode(ContainerModel containerModel, JobContext jobContext,
      ContainerContext containerContext, Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Map<String, Serde<Object>> serdes, Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics,
      Map<TaskName, TaskInstanceCollector> taskInstanceCollectors) {

    // iterate over each task and each storeName
    for (Map.Entry<TaskName, TaskModel> task : containerModel.getTasks().entrySet()) {
      TaskName taskName = task.getKey();
      TaskModel taskModel = task.getValue();
      Map<String, StorageEngine> nonSideInputStores = getNonSideInputNonDaVinciStores(taskName);

      for (String storeName : nonSideInputStores.keySet()) {

        // if this store has been already created then re-create and overwrite it only if it is a
        // persistentStore and a non-sideInputStore, because sideInputStores are always created in RW mode
        if (nonSideInputStores.get(storeName).getStoreProperties().isPersistedToDisk()) {

          StorageEngine storageEngine =
              createStore(storeName, taskName, taskModel, jobContext, containerContext, storageEngineFactories, serdes, taskInstanceMetrics, taskInstanceCollectors,
                  StorageEngineFactory.StoreMode.ReadWrite);

          // add created store to map
          this.taskStores.get(taskName).put(storeName, storageEngine);

          LOG.info("Re-created store {} in read-write mode for task {} because it a persistent store", storeName, taskName);
        } else {
          LOG.info("Skipping re-creation of store {} for task {}", storeName, taskName);
        }
      }
    }
  }

  /**
   * Method to instantiate a StorageEngine with the given parameters, and populate the storeDirectory paths (used to monitor
   * disk space).
   */
  private StorageEngine createStore(String storeName, TaskName taskName, TaskModel taskModel, JobContext jobContext,
      ContainerContext containerContext, Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Map<String, Serde<Object>> serdes, Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics,
      Map<TaskName, TaskInstanceCollector> taskInstanceCollectors, StorageEngineFactory.StoreMode storeMode) {

    StorageConfig storageConfig = new StorageConfig(config);

    SystemStreamPartition changeLogSystemStreamPartition =
        (changelogSystemStreams.containsKey(storeName)) ? new SystemStreamPartition(
            changelogSystemStreams.get(storeName), taskModel.getChangelogPartition()) : null;

    // Use the logged-store-base-directory for change logged stores and sideInput stores, and non-logged-store-base-dir
    // for non logged stores
    File storeDirectory;
    if (changeLogSystemStreamPartition != null || this.taskSideInputStoreSSPs.get(taskName).containsKey(storeName)) {
      storeDirectory = storageManagerUtil.getTaskStoreDir(this.loggedStoreBaseDirectory, storeName, taskName,
          taskModel.getTaskMode());
    } else {
      storeDirectory = storageManagerUtil.getTaskStoreDir(this.nonLoggedStoreBaseDirectory, storeName, taskName,
          taskModel.getTaskMode());
    }

    this.storeDirectoryPaths.add(storeDirectory.toPath());

    Optional<String> storageKeySerde = storageConfig.getStorageKeySerde(storeName);
    Serde keySerde = null;
    if (storageKeySerde.isPresent()) {
      keySerde = serdes.get(storageKeySerde.get());
    }
    Optional<String> storageMsgSerde = storageConfig.getStorageMsgSerde(storeName);
    Serde messageSerde = null;
    if (storageMsgSerde.isPresent()) {
      messageSerde = serdes.get(storageMsgSerde.get());
    }

    // if taskInstanceMetrics are specified use those for store metrics,
    // otherwise (in case of StorageRecovery) use a blank MetricsRegistryMap
    MetricsRegistry storeMetricsRegistry =
        taskInstanceMetrics.get(taskName) != null ? taskInstanceMetrics.get(taskName).registry()
            : new MetricsRegistryMap();

    return storageEngineFactories.get(storeName)
        .getStorageEngine(storeName, taskModel, storeDirectory, keySerde, messageSerde, taskInstanceCollectors.get(taskName),
            storeMetricsRegistry, changeLogSystemStreamPartition, jobContext, containerContext, storeMode);
  }


  // Create sideInput store processors, one per store per task
  private Map<TaskName, Map<String, SideInputsProcessor>> createSideInputProcessors(StorageConfig config,
      ContainerModel containerModel, Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics) {

    Map<TaskName, Map<String, SideInputsProcessor>> sideInputStoresToProcessors = new HashMap<>();
    containerModel.getTasks().forEach((taskName, taskModel) -> {
      sideInputStoresToProcessors.put(taskName, new HashMap<>());
      TaskMode taskMode = taskModel.getTaskMode();

      for (String storeName : this.taskSideInputStoreSSPs.get(taskName).keySet()) {

        SideInputsProcessor sideInputsProcessor;
        Optional<String> sideInputsProcessorSerializedInstance =
            config.getSideInputsProcessorSerializedInstance(storeName);

        if (sideInputsProcessorSerializedInstance.isPresent()) {

          sideInputsProcessor = SerdeUtils.deserialize("Side Inputs Processor", sideInputsProcessorSerializedInstance.get());
          LOG.info("Using serialized side-inputs-processor for store: {}, task: {}", storeName, taskName);

        } else if (config.getSideInputsProcessorFactory(storeName).isPresent()) {
          String sideInputsProcessorFactoryClassName = config.getSideInputsProcessorFactory(storeName).get();
          SideInputsProcessorFactory sideInputsProcessorFactory =
              ReflectionUtil.getObj(sideInputsProcessorFactoryClassName, SideInputsProcessorFactory.class);
          sideInputsProcessor = sideInputsProcessorFactory.getSideInputsProcessor(config, taskInstanceMetrics.get(taskName).registry());
          LOG.info("Using side-inputs-processor from factory: {} for store: {}, task: {}", config.getSideInputsProcessorFactory(storeName).get(), storeName, taskName);

        } else {
          // if this is a active-task with a side-input store but no sideinput-processor-factory defined in config, we rely on upstream validations to fail the deploy

          // if this is a standby-task and the store is a non-side-input changelog store
          // we creating identity sideInputProcessor for stores of standbyTasks
          // have to use the right serde because the sideInput stores are created

          Serde keySerde = serdes.get(config.getStorageKeySerde(storeName)
              .orElseThrow(() -> new SamzaException("Could not find storage key serde for store: " + storeName)));
          Serde msgSerde = serdes.get(config.getStorageMsgSerde(storeName)
              .orElseThrow(() -> new SamzaException("Could not find storage msg serde for store: " + storeName)));
          sideInputsProcessor = new SideInputsProcessor() {
            @Override
            public Collection<Entry<?, ?>> process(IncomingMessageEnvelope message, KeyValueStore store) {
              // Ignore message if the key is null
              if (message.getKey() == null) {
                return ImmutableList.of();
              } else {
                // Skip serde if the message is null
                return ImmutableList.of(new Entry<>(keySerde.fromBytes((byte[]) message.getKey()),
                    message.getMessage() == null ? null : msgSerde.fromBytes((byte[]) message.getMessage())));
              }
            }
          };
          LOG.info("Using identity side-inputs-processor for store: {}, task: {}", storeName, taskName);
        }

        sideInputStoresToProcessors.get(taskName).put(storeName, sideInputsProcessor);
      }
    });

    return sideInputStoresToProcessors;
  }

  // Create task sideInput storage managers, one per task, index by the SSP they are responsible for consuming
  private Map<SystemStreamPartition, TaskSideInputHandler> createSideInputHandlers(Clock clock) {
    // creating sideInput store processors, one per store per task
    Map<TaskName, Map<String, SideInputsProcessor>> taskSideInputProcessors =
        createSideInputProcessors(new StorageConfig(config), this.containerModel, this.taskInstanceMetrics);

    Map<SystemStreamPartition, TaskSideInputHandler> handlers = new HashMap<>();

    if (this.hasSideInputs) {
      containerModel.getTasks().forEach((taskName, taskModel) -> {

        Map<String, StorageEngine> sideInputStores = getSideInputStores(taskName);
        Map<String, Set<SystemStreamPartition>> sideInputStoresToSSPs = new HashMap<>();
        boolean taskHasSideInputs = false;
        for (String storeName : sideInputStores.keySet()) {
          Set<SystemStreamPartition> storeSSPs = this.taskSideInputStoreSSPs.get(taskName).get(storeName);
          taskHasSideInputs = taskHasSideInputs || !storeSSPs.isEmpty();
          sideInputStoresToSSPs.put(storeName, storeSSPs);
        }

        if (taskHasSideInputs) {
          CountDownLatch taskCountDownLatch = new CountDownLatch(1);
          this.sideInputTaskLatches.put(taskName, taskCountDownLatch);

          TaskSideInputHandler taskSideInputHandler = new TaskSideInputHandler(taskName,
              taskModel.getTaskMode(),
              loggedStoreBaseDirectory,
              sideInputStores,
              sideInputStoresToSSPs,
              taskSideInputProcessors.get(taskName),
              this.systemAdmins,
              this.streamMetadataCache,
              taskCountDownLatch,
              clock);

          sideInputStoresToSSPs.values().stream().flatMap(Set::stream).forEach(ssp -> {
            handlers.put(ssp, taskSideInputHandler);
          });

          LOG.info("Created TaskSideInputHandler for task {}, sideInputStores {} and loggedStoreBaseDirectory {}",
              taskName, sideInputStores, loggedStoreBaseDirectory);
        }
      });
    }
    return handlers;
  }

  private Map<String, StorageEngine> getSideInputStores(TaskName taskName) {
    return taskStores.get(taskName).entrySet().stream().
        filter(e -> this.taskSideInputStoreSSPs.get(taskName).containsKey(e.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Map<String, StorageEngine> getNonSideInputStores(TaskName taskName) {
    return taskStores.get(taskName).entrySet().stream().
        filter(e -> !this.taskSideInputStoreSSPs.get(taskName).containsKey(e.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Map<String, StorageEngine> getNonSideInputNonDaVinciStores(TaskName taskName) {
    StorageConfig storageConfig = new StorageConfig(config);
    return getNonSideInputStores(taskName).entrySet()
        .stream()
        .filter(e -> !StringUtils.equals(storageConfig.getStorageFactoryClassName(e.getKey()).get(),
            DAVINCI_KV_STORAGE_ENGINE_FACTORY))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Set<TaskSideInputHandler> getSideInputHandlers() {
    return this.sspSideInputHandlers.values().stream().collect(Collectors.toSet());
  }

  public void start() throws SamzaException, InterruptedException {
    Map<SystemStreamPartition, String> checkpointedChangelogSSPOffsets = new HashMap<>();
    if (new TaskConfig(config).getTransactionalStateRestoreEnabled()) {
      getTasks(containerModel, TaskMode.Active).forEach((taskName, taskModel) -> {
        if (checkpointManager != null) {
          Set<SystemStream> changelogSystemStreams = new HashSet<>(this.changelogSystemStreams.values());
          Checkpoint checkpoint = checkpointManager.readLastCheckpoint(taskName);
          if (checkpoint != null) {
            checkpoint.getOffsets().forEach((ssp, offset) -> {
              if (changelogSystemStreams.contains(new SystemStream(ssp.getSystem(), ssp.getStream()))) {
                checkpointedChangelogSSPOffsets.put(ssp, offset);
              }
            });
          }
        }
      });
    }

    // Restore needs to happen first before the store init
    LOG.info("Checkpointed changelog ssp offsets: {}", checkpointedChangelogSSPOffsets);
    restoreStores(checkpointedChangelogSSPOffsets);
    // Init Stores is called by CSM after restoration because restore is a Samza added abstraction
    // This assumes that for any restoration requirements a store does not need to be constructed
    initStores();
    if (this.hasSideInputs) {
      startSideInputs();
    }
  }


  private void initStores() throws InterruptedException {
    LOG.info("Store Init started");

    // Create a thread pool for parallel restores (and stopping of persistent stores)
    // TODO: Add a config to use init threadpool
    ExecutorService executorService = Executors.newFixedThreadPool(this.parallelInitThreadPoolSize,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat(STORE_INIT_THREAD_NAME).build());

    List<Future> taskInitFutures = new ArrayList<>(this.taskStores.entrySet().size());

    // Submit restore callable for each taskInstance
    this.taskStores.forEach((taskName, taskStores) -> {
      taskInitFutures.add(executorService.submit(
          new TaskInitCallable(this.samzaContainerMetrics, taskName, taskStores)));
    });

    // loop-over the future list to wait for each thread to finish, catch any exceptions during restore and throw
    // as samza exceptions
    for (Future future : taskInitFutures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        LOG.warn("Received an interrupt during store init. Issuing interrupts to the store init workers to exit "
            + "prematurely without completing the init of state store");
        executorService.shutdownNow();
        throw e;
      } catch (Exception e) {
        LOG.error("Exception during store init ", e);
        throw new SamzaException("Exception during store init ", e);
      }
    }

    executorService.shutdown();

    LOG.info("Store Init complete");
  }

  // Restoration of all stores, in parallel across tasks
  private void restoreStores(Map<SystemStreamPartition, String> checkpointedChangelogSSPOffsets)
      throws InterruptedException {
    LOG.info("Store Restore started");

    // initialize each TaskStorageManager
    this.taskRestoreManagers.values().forEach(taskStorageManager ->
       taskStorageManager.init(checkpointedChangelogSSPOffsets));

    // Start each store consumer once
    this.storeConsumers.values().stream().distinct().forEach(SystemConsumer::start);

    // Create a thread pool for parallel restores (and stopping of persistent stores)
    ExecutorService executorService = Executors.newFixedThreadPool(this.parallelRestoreThreadPoolSize,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat(RESTORE_THREAD_NAME).build());

    List<Future> taskRestoreFutures = new ArrayList<>(this.taskRestoreManagers.entrySet().size());

    // Submit restore callable for each taskInstance
    this.taskRestoreManagers.forEach((taskInstance, taskRestoreManager) -> {
      taskRestoreFutures.add(executorService.submit(
          new TaskRestoreCallable(this.samzaContainerMetrics, taskInstance, taskRestoreManager)));
    });

    // loop-over the future list to wait for each thread to finish, catch any exceptions during restore and throw
    // as samza exceptions
    for (Future future : taskRestoreFutures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        LOG.warn("Received an interrupt during store restoration. Issuing interrupts to the store restoration workers to exit "
            + "prematurely without restoring full state.");
        executorService.shutdownNow();
        throw e;
      } catch (Exception e) {
        LOG.error("Exception when restoring ", e);
        throw new SamzaException("Exception when restoring ", e);
      }
    }

    executorService.shutdown();

    // Stop each store consumer once
    this.storeConsumers.values().stream().distinct().forEach(SystemConsumer::stop);

    // Now re-create persistent stores in read-write mode, leave non-persistent stores as-is
    recreatePersistentTaskStoresInReadWriteMode(this.containerModel, jobContext, containerContext,
        storageEngineFactories, serdes, taskInstanceMetrics, taskInstanceCollectors);

    LOG.info("Store Restore complete");
  }

  // Read sideInputs until all sideInputStreams are caughtup, so start() can return
  private void startSideInputs() {

    LOG.info("SideInput Restore started");

    // initialize the sideInputStorageManagers
    getSideInputHandlers().forEach(TaskSideInputHandler::init);

    Map<TaskName, TaskSideInputHandler> taskSideInputHandlers = this.sspSideInputHandlers.values().stream()
        .distinct()
        .collect(Collectors.toMap(TaskSideInputHandler::getTaskName, Function.identity()));

    Map<TaskName, TaskInstanceMetrics> sideInputTaskMetrics = new HashMap<>();
    Map<TaskName, RunLoopTask> sideInputTasks = new HashMap<>();
    this.taskSideInputStoreSSPs.forEach((taskName, storesToSSPs) -> {
      Set<SystemStreamPartition> taskSSPs = this.taskSideInputStoreSSPs.get(taskName).values().stream()
          .flatMap(Set::stream)
          .collect(Collectors.toSet());

      if (!taskSSPs.isEmpty()) {
        String sideInputSource = SIDEINPUTS_METRICS_PREFIX + this.taskInstanceMetrics.get(taskName).source();
        TaskInstanceMetrics sideInputMetrics = new TaskInstanceMetrics(sideInputSource, this.taskInstanceMetrics.get(taskName).registry(), SIDEINPUTS_METRICS_PREFIX);
        sideInputTaskMetrics.put(taskName, sideInputMetrics);

        RunLoopTask sideInputTask = new SideInputTask(taskName, taskSSPs, taskSideInputHandlers.get(taskName), sideInputTaskMetrics.get(taskName));
        sideInputTasks.put(taskName, sideInputTask);
      }
    });

    // register all sideInput SSPs with the consumers
    for (SystemStreamPartition ssp : this.sspSideInputHandlers.keySet()) {
      String startingOffset = this.sspSideInputHandlers.get(ssp).getStartingOffset(ssp);

      if (startingOffset == null) {
        throw new SamzaException(
            "No starting offset could be obtained for SideInput SystemStreamPartition : " + ssp + ". Consumer cannot start.");
      }

      // register startingOffset with the sysConsumer and register a metric for it
      sideInputSystemConsumers.register(ssp, startingOffset);
      taskInstanceMetrics.get(this.sspSideInputHandlers.get(ssp).getTaskName()).addOffsetGauge(
          ssp, ScalaJavaUtil.toScalaFunction(() -> this.sspSideInputHandlers.get(ssp).getLastProcessedOffset(ssp)));
      sideInputTaskMetrics.get(this.sspSideInputHandlers.get(ssp).getTaskName()).addOffsetGauge(
          ssp, ScalaJavaUtil.toScalaFunction(() -> this.sspSideInputHandlers.get(ssp).getLastProcessedOffset(ssp)));
    }

    // start the systemConsumers for consuming input
    this.sideInputSystemConsumers.start();

    TaskConfig taskConfig = new TaskConfig(this.config);
    SamzaContainerMetrics sideInputContainerMetrics =
        new SamzaContainerMetrics(SIDEINPUTS_METRICS_PREFIX + this.samzaContainerMetrics.source(),
            this.samzaContainerMetrics.registry(), SIDEINPUTS_METRICS_PREFIX);

    this.sideInputRunLoop = new RunLoop(sideInputTasks,
        null, // all operations are executed in the main runloop thread
        this.sideInputSystemConsumers,
        1, // single message in flight per task
        -1, // no windowing
        taskConfig.getCommitMs(),
        taskConfig.getCallbackTimeoutMs(),
        // TODO consolidate these container configs SAMZA-2275
        this.config.getLong("container.disk.quota.delay.max.ms", TimeUnit.SECONDS.toMillis(1)),
        taskConfig.getMaxIdleMs(),
        sideInputContainerMetrics,
        System::nanoTime,
        false); // commit must be synchronous to ensure integrity of state flush

    try {
      sideInputsExecutor.submit(() -> {
        try {
          sideInputRunLoop.run();
        } catch (Exception e) {
          LOG.error("Exception in reading sideInputs", e);
          sideInputException = e;
        }
      });

      // Make the main thread wait until all sideInputs have been caughtup or an exception was thrown
      while (!shouldShutdown && sideInputException == null &&
          !awaitSideInputTasks()) {
        LOG.debug("Waiting for SideInput bootstrap to complete");
      }

      if (sideInputException != null) { // Throw exception if there was an exception in catching-up sideInputs
        throw new SamzaException("Exception in restoring sideInputs", sideInputException);
      }

    } catch (InterruptedException e) {
      LOG.warn("Received an interrupt during side inputs store restoration."
          + " Exiting prematurely without completing store restore.");
      /*
       * We want to stop side input restoration and rethrow the exception upstream. Container should handle the
       * interrupt exception and shutdown the components and cleaning up the resource. We don't want to clean up the
       * resources prematurely here.
       */
      shouldShutdown = true; // todo: should we cancel the flush future right away or wait for container to handle it as part of shutdown sequence?
      throw new SamzaException("Side inputs read was interrupted", e);
    }

    LOG.info("SideInput Restore complete");
  }

  /**
   * Waits for all side input tasks to catch up until a timeout.
   *
   * @return False if waiting on any latch timed out, true otherwise
   *
   * @throws InterruptedException if waiting any of the latches is interrupted
   */
  private boolean awaitSideInputTasks() throws InterruptedException {
    long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(SIDE_INPUT_CHECK_TIMEOUT_SECONDS);
    for (CountDownLatch latch : this.sideInputTaskLatches.values()) {
      long remainingMillisToWait = endTime - System.currentTimeMillis();
      if (remainingMillisToWait <= 0 || !latch.await(remainingMillisToWait, TimeUnit.MILLISECONDS)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get the {@link StorageEngine} instance with a given name for a given task.
   * @param taskName the task name for which the storage engine is desired.
   * @param storeName the desired store's name.
   * @return the task store.
   */
  public Optional<StorageEngine> getStore(TaskName taskName, String storeName) {
    return Optional.ofNullable(this.taskStores.get(taskName).get(storeName));
  }

  /**
   *  Get all {@link StorageEngine} instance used by a given task.
   * @param taskName  the task name, all stores for which are desired.
   * @return map of stores used by the given task, indexed by storename
   */
  public Map<String, StorageEngine> getAllStores(TaskName taskName) {
    return this.taskStores.get(taskName);
  }

  /**
   * Get all {@link StorageEngine} instance used by a given task exceptDaVinciStores
   * @param taskName
   * @return
   */
  public Map<String, StorageEngine> getNonDaVinciStores(TaskName taskName) {
    StorageConfig storageConfig = new StorageConfig(config);
    return taskStores.get(taskName).entrySet().stream().
        filter(e -> !StringUtils.equals(
            storageConfig.getStorageFactoryClassName(e.getKey()).get(), DAVINCI_KV_STORAGE_ENGINE_FACTORY)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Set of directory paths for all stores restored by this {@link ContainerStorageManager}.
   * @return the set of all store directory paths
   */
  public Set<Path> getStoreDirectoryPaths() {
    return this.storeDirectoryPaths;
  }

  @VisibleForTesting
  public void stopStores() {
    this.taskStores.forEach((taskName, storeMap) -> storeMap.forEach((storeName, store) -> store.stop()));
  }

  public void shutdown() {
    // stop all nonsideinputstores including persistent and non-persistent stores
    this.containerModel.getTasks().forEach((taskName, taskModel) ->
        getNonSideInputStores(taskName).forEach((storeName, store) -> store.stop())
    );

    this.shouldShutdown = true;

    // stop all sideinput consumers and stores
    if (this.hasSideInputs) {
      this.sideInputRunLoop.shutdown();
      this.sideInputsExecutor.shutdown();
      try {
        this.sideInputsExecutor.awaitTermination(SIDE_INPUT_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new SamzaException("Exception while shutting down sideInputs", e);
      }

      this.sideInputSystemConsumers.stop();

      // stop all sideInputStores -- this will perform one last flush on the KV stores, and write the offset file
      this.getSideInputHandlers().forEach(TaskSideInputHandler::stop);
    }
    LOG.info("Shutdown complete");
  }

  /**
   * Callable for performing the restore on a task restore manager and emitting the task-restoration metric.
   * After restoration, all persistent stores are stopped (which will invoke compaction in case of certain persistent
   * stores that were opened in bulk-load mode).
   * Performing stop here parallelizes this compaction, which is a time-intensive operation.
   *
   */
  private class TaskRestoreCallable implements Callable<Void> {

    private TaskName taskName;
    private TaskRestoreManager taskRestoreManager;
    private SamzaContainerMetrics samzaContainerMetrics;

    public TaskRestoreCallable(SamzaContainerMetrics samzaContainerMetrics, TaskName taskName,
        TaskRestoreManager taskRestoreManager) {
      this.samzaContainerMetrics = samzaContainerMetrics;
      this.taskName = taskName;
      this.taskRestoreManager = taskRestoreManager;
    }

    @Override
    public Void call() {
      long startTime = System.currentTimeMillis();
      try {
        LOG.info("Starting stores in task instance {}", this.taskName.getTaskName());
        taskRestoreManager.restore();
      } catch (InterruptedException e) {
        /*
         * The container thread is the only external source to trigger an interrupt to the restoration thread and thus
         * it is okay to swallow this exception and not propagate it upstream. If the container is interrupted during
         * the store restoration, ContainerStorageManager signals the restore workers to abandon restoration and then
         * finally propagates the exception upstream to trigger container shutdown.
         */
        LOG.warn("Received an interrupt during store restoration for task: {}.", this.taskName.getTaskName());
      } finally {
        // Stop all persistent stores after restoring. Certain persistent stores opened in BulkLoad mode are compacted
        // on stop, so paralleling stop() also parallelizes their compaction (a time-intensive operation).
        taskRestoreManager.stopPersistentStores();
        long timeToRestore = System.currentTimeMillis() - startTime;

        if (this.samzaContainerMetrics != null) {
          Gauge taskGauge = this.samzaContainerMetrics.taskStoreRestorationMetrics().getOrDefault(this.taskName, null);

          if (taskGauge != null) {
            taskGauge.set(timeToRestore);
          }
        }
      }

      return null;
    }
  }

  /**
   * Callable for performing the init on a StorageEngine
   *
   */
  private class TaskInitCallable implements Callable<Void> {

    private TaskName taskName;
    private Map<String, StorageEngine> taskStores;
    private SamzaContainerMetrics samzaContainerMetrics;

    public TaskInitCallable(SamzaContainerMetrics samzaContainerMetrics, TaskName taskName,
        Map<String, StorageEngine> taskStores) {
      this.samzaContainerMetrics = samzaContainerMetrics;
      this.taskName = taskName;
      this.taskStores = taskStores;
    }

    @Override
    public Void call() {
      long startTime = System.currentTimeMillis();
      try {
        LOG.info("Starting store init in task instance {}", this.taskName.getTaskName());
        for (Map.Entry<String, StorageEngine> store : taskStores.entrySet()) {
          store.getValue().init(context);
        }
      }
      finally {
        long timeToInit = System.currentTimeMillis() - startTime;
        if (this.samzaContainerMetrics != null) {
          Gauge taskGauge = this.samzaContainerMetrics.taskStoreInitMetrics().getOrDefault(this.taskName, null);
          if (taskGauge != null) {
            taskGauge.set(timeToInit);
          }
        }
      }

      return null;
    }
  }

}
