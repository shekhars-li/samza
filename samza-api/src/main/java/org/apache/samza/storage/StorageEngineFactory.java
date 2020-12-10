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

import java.io.File;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;

/**
 * An object provided by the storage engine implementation to create instances
 * of the given storage engine type.
 */
public interface StorageEngineFactory<K, V> {

  /**
   * Enum to describe different modes a {@link StorageEngine} can be created in.
   * The BulkLoad mode is used when bulk loading of data onto the store, e.g., store-restoration at Samza
   * startup. In this mode, the underlying store will tailor itself for write-intensive ops -- tune its params,
   * adapt its compaction behaviour, etc.
   *
   * The ReadWrite mode is used for normal read-write ops by the application.
   */
  enum StoreMode {
    BulkLoad("bulk"), ReadWrite("rw");
    public final String mode;

    StoreMode(String mode) {
      this.mode = mode;
    }
  }

  /**
   * Create an instance of the given storage engine.
   *
   * @param storeName The name of the storage engine.
   * @param taskModel Task Model for the task
   * @param storeDir The directory of the storage engine.
   * @param keySerde The serializer to use for serializing keys when reading or writing to the store.
   * @param msgSerde The serializer to use for serializing messages when reading or writing to the store.
   * @param collector MessageCollector the storage engine uses to persist changes.
   * @param registry MetricsRegistry to which to publish storage-engine specific metrics.
   * @param changeLogSystemStreamPartition Samza stream partition from which to receive the changelog.
   * @param jobContext Information about the job in which the task is executing
   * @param containerContext Information about the container in which the task is executing.
   * @param storeMode The mode in which the instance should be created in.
   * @return The storage engine instance.
   */
  StorageEngine getStorageEngine(
    String storeName,
    TaskModel taskModel,
    File storeDir,
    Serde<K> keySerde,
    Serde<V> msgSerde,
    MessageCollector collector,
    MetricsRegistry registry,
    SystemStreamPartition changeLogSystemStreamPartition,
    JobContext jobContext,
    ContainerContext containerContext, StoreMode storeMode);
}
