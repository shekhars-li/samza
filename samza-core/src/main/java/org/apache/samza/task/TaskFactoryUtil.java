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
package org.apache.samza.task;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.application.ApplicationDescriptor;
import org.apache.samza.application.ApplicationDescriptorImpl;
import org.apache.samza.application.LegacyTaskApplication;
import org.apache.samza.application.StreamApplicationDescriptorImpl;
import org.apache.samza.application.TaskApplicationDescriptorImpl;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.task.wrapper.SubTaskWrapperTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * This class provides utility functions to load task factory classes based on config, and to wrap {@link StreamTaskFactory}
 * in {@link AsyncStreamTaskFactory} when running {@link StreamTask}s in multi-thread mode
 */
public class TaskFactoryUtil {
  private static final Logger log = LoggerFactory.getLogger(TaskFactoryUtil.class);

  /**
   * Creates a {@link TaskFactory} based on {@link ApplicationDescriptorImpl}
   *
   * @param appDesc {@link ApplicationDescriptorImpl} for this application
   * @return {@link TaskFactory} object defined by {@code appDesc}
   */
  public static TaskFactory getTaskFactory(ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc) {
    if (appDesc instanceof TaskApplicationDescriptorImpl) {
      // NOTE: LinkedIn only: for users using OffspringHelper, we may need to instantiate wrapper task class.
      return maybeWrapperTaskApplicationTaskFactory((TaskApplicationDescriptorImpl) appDesc);
    } else if (appDesc instanceof StreamApplicationDescriptorImpl) {
      // NOTE: LinkedIn only: for users using OffspringHelper, we may need to instantiate wrapper task class.
      return maybeWrapperStreamApplicationTaskFactory((StreamApplicationDescriptorImpl) appDesc);
    }
    throw new IllegalArgumentException(String.format("ApplicationDescriptorImpl has to be either TaskApplicationDescriptorImpl or "
        + "StreamApplicationDescriptorImpl. class %s is not supported", appDesc.getClass().getName()));
  }

  /**
   * Creates a {@link TaskFactory} based on the configuration.
   * <p>
   * This should only be used to create {@link TaskFactory} defined in task.class
   *
   * @param taskClassName  the task class name for this job
   * @return  a {@link TaskFactory} object, either a instance of {@link StreamTaskFactory} or {@link AsyncStreamTaskFactory}
   */
  public static TaskFactory getTaskFactory(String taskClassName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(taskClassName), "task.class cannot be empty");
    log.info("Got task class name: {}", taskClassName);

    boolean isAsyncTaskClass;
    try {
      isAsyncTaskClass = AsyncStreamTask.class.isAssignableFrom(Class.forName(taskClassName));
    } catch (Throwable t) {
      throw new ConfigException(String.format("Invalid configuration for AsyncStreamTask class: %s", taskClassName), t);
    }

    if (isAsyncTaskClass) {
      return (AsyncStreamTaskFactory) () -> {
        try {
          return (AsyncStreamTask) Class.forName(taskClassName).newInstance();
        } catch (Throwable t) {
          log.error("Error loading AsyncStreamTask class: {}. error: {}", taskClassName, t);
          throw new SamzaException(String.format("Error loading AsyncStreamTask class: %s", taskClassName), t);
        }
      };
    }

    return (StreamTaskFactory) () -> {
      try {
        return (StreamTask) Class.forName(taskClassName).newInstance();
      } catch (Throwable t) {
        log.error("Error loading StreamTask class: {}. error: {}", taskClassName, t);
        throw new SamzaException(String.format("Error loading StreamTask class: %s", taskClassName), t);
      }
    };
  }

  /**
   * Optionally wrap the {@link StreamTaskFactory} in a {@link AsyncStreamTaskFactory}, when running {@link StreamTask}
   * in multi-thread mode.
   *
   * @param factory  the task factory instance loaded according to the task class
   * @param singleThreadMode  the flag indicating whether the job is running in single thread mode or not
   * @param taskThreadPool  the thread pool to run the {@link AsyncStreamTaskAdapter} tasks
   * @return  the finalized task factory object
   */
  public static TaskFactory finalizeTaskFactory(TaskFactory factory, boolean singleThreadMode, ExecutorService taskThreadPool) {

    validateFactory(factory);

    boolean isAsyncTaskClass = factory instanceof AsyncStreamTaskFactory;
    if (isAsyncTaskClass) {
      log.info("Got an AsyncStreamTask implementation.");
    }

    if (singleThreadMode && isAsyncTaskClass) {
      throw new SamzaException("AsyncStreamTask cannot run on single thread mode.");
    }

    if (!singleThreadMode && !isAsyncTaskClass) {
      log.info("Converting StreamTask to AsyncStreamTaskAdapter when running StreamTask with multiple threads");
      return (AsyncStreamTaskFactory) () -> new AsyncStreamTaskAdapter(((StreamTaskFactory) factory).createInstance(), taskThreadPool);
    }

    return factory;
  }

  private static void validateFactory(TaskFactory factory) {
    if (factory == null) {
      throw new SamzaException("Either the task class name or the task factory instance is required.");
    }

    if (!(factory instanceof StreamTaskFactory) && !(factory instanceof AsyncStreamTaskFactory)) {
      throw new SamzaException(String.format("TaskFactory must be either StreamTaskFactory or AsyncStreamTaskFactory. %s is not supported",
          factory.getClass()));
    }
  }

  // NOTE: LinkedIn only. For users using OffspringHelper and the high-level API, we may need to create wrapper task factory.
  private static TaskFactory maybeWrapperStreamApplicationTaskFactory(StreamApplicationDescriptorImpl streamAppDesc) {
    StreamTaskFactory taskFactory = (StreamTaskFactory) () ->
        new StreamOperatorTask(streamAppDesc.getOperatorSpecGraph(), streamAppDesc.getContextManager());
    String wrapperTaskClassName = getWrapperTaskClassName(streamAppDesc, true /* is StreamTask */);
    if (StringUtils.isNotBlank(wrapperTaskClassName)) {
      return createStreamTaskWrapperTaskFactory(taskFactory, wrapperTaskClassName);
    }
    return taskFactory;
  }

  // NOTE: LinkedIn only. For users using OffspringHelper and the high-level API, we may need to create wrapper task factory.
  private static TaskFactory maybeWrapperTaskApplicationTaskFactory(TaskApplicationDescriptorImpl taskAppDesc) {
    TaskFactory taskFactory = taskAppDesc.getTaskFactory();
    if (LegacyTaskApplication.class.isAssignableFrom(taskAppDesc.getAppClass())) {
      // For LegacyTaskApplication (i.e. task applications w/o app.class), we honor the existing task.class + task.subtask.class
      // implementation, which does not need to wrap the task factory.
      return taskFactory;
    }
    // For task applications w/ app.class configuration, we use task.class as the wrapper class.
    String wrapperTaskClassName = getWrapperTaskClassName(taskAppDesc, taskFactory instanceof StreamTaskFactory);
    if (StringUtils.isNotBlank(wrapperTaskClassName)) {
      if (taskFactory instanceof StreamTaskFactory) {
        return createStreamTaskWrapperTaskFactory((StreamTaskFactory) taskFactory, wrapperTaskClassName);
      } else if (taskFactory instanceof AsyncStreamTaskFactory){
        // must be AsyncStreamTaskFactory
        return createAsyncStreamTaskWrapperTaskFactory((AsyncStreamTaskFactory) taskFactory, wrapperTaskClassName);
      }
      throw new IllegalArgumentException(String.format("TaskFactory class %s is not supported", taskFactory.getClass().getName()));
    }
    return taskFactory;
  }

  // NOTE: LinkedIn only. For users using OffspringHelper and the high-level API, we need to validate the wrapper task class
  // from task.class is a sub-class of SubTaskWrapperTask.
  private static String getWrapperTaskClassName(ApplicationDescriptorImpl appDesc, boolean isStreamTask) {
    Config config = appDesc.getConfig();
    String wrapperClassName = isStreamTask ? config.get(SubTaskWrapperTask.STREAM_TASK_WRAPPER_CLASS_CFG) :
        config.get(SubTaskWrapperTask.ASYNC_STREAM_TASK_WRAPPER_CLASS_CFG);
    // For high-level API applications, the wrapper class has to extend SubTaskWrapperTask
    try {
      if (StringUtils.isNotBlank(wrapperClassName) &&
          !SubTaskWrapperTask.class.isAssignableFrom(Class.forName(wrapperClassName))) {
        throw new ConfigException(String.format("SamzaApplication cannot be wrapped by "
            + "a wrapper class {}", wrapperClassName));
      }
    } catch (ClassNotFoundException e) {
      throw new ConfigException(String.format("Can't find the wrapper class {}", wrapperClassName), e);
    }
    return wrapperClassName;
  }

  // NOTE: LinkedIn only. For users using OffspringHelper and the high-level API, we need to create a wrapper task factory
  // with task class StreamTaskSubTaskServiceCallWrapper.
  private static StreamTaskFactory createStreamTaskWrapperTaskFactory(
      StreamTaskFactory streamTaskFactory, String wrapperClassName) {
    return () -> {
      // If the job is using OffspringHelper, LiSamzaRewriter sets task.class
      // to a wrapper class StreamTaskSubTaskServiceCallWrapper. If so, use that instead.
      try {
        return (StreamTask) Class.forName(wrapperClassName)
            .getConstructor(StreamTask.class)
            .newInstance(streamTaskFactory.createInstance());
      } catch (Exception e) {
        throw new SamzaException("Could not instantiate wrapper task class for OffSpringHelper.", e);
      }
    };
  }

  // NOTE: LinkedIn only. For users using OffspringHelper and the high-level API, we need to create a wrapper task factory
  // with task class StreamTaskSubTaskServiceCallWrapper.
  private static AsyncStreamTaskFactory createAsyncStreamTaskWrapperTaskFactory(
      AsyncStreamTaskFactory asyncStreamTaskFactory, String wrapperClassName) {
    return () -> {
      // If the job is using OffspringHelper, LiSamzaRewriter sets task.class
      // to a wrapper class StreamTaskSubTaskServiceCallWrapper. If so, use that instead.
      try {
        return (AsyncStreamTask) Class.forName(wrapperClassName)
            .getConstructor(AsyncStreamTask.class)
            .newInstance(asyncStreamTaskFactory.createInstance());
      } catch (Exception e) {
        throw new SamzaException("Could not instantiate wrapper task class for OffSpringHelper.", e);
      }
    };
  }
}
