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
package org.apache.samza.task.wrapper;

import org.apache.samza.context.Context;
import org.apache.samza.task.ClosableTask;
import org.apache.samza.task.EndOfStreamListenerTask;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;


/**
 * NOTE: LinkedIn specific need to allow a wrapper task class for tasks that use OffspringHelper.
 * This extended class will have to implement a constructor that takes a {@link StreamTask} as parameter.
 */
public abstract class TaskWrapperBase<T> implements InitableTask, WindowableTask, ClosableTask, EndOfStreamListenerTask {
  protected abstract T getTask();

  @Override
  public void init(Context context) throws Exception {
    T task = getTask();
    if (task instanceof InitableTask) {
      ((InitableTask) task).init(context);
    }
  }

  @Override
  public void close() throws Exception {
    T task = getTask();
    if (task instanceof ClosableTask) {
      ((ClosableTask) task).close();
    }
  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    T task = getTask();
    if (task instanceof WindowableTask) {
      ((WindowableTask) task).window(collector, coordinator);
    }
  }

  @Override
  public void onEndOfStream(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    T task = getTask();
    if (task instanceof EndOfStreamListenerTask) {
      ((EndOfStreamListenerTask) task).onEndOfStream(collector, coordinator);
    }
  }

}
