package org.apache.samza.task.wrapper;

import org.apache.samza.config.Config;


/**
 * NOTE: LinkedIn only. This class is the base class to allow wrapper task to be constructed with an {@code subTask} instance
 */
public abstract class SubTaskWrapperTask<T> extends TaskWrapperBase<T> {
  private final T task;

  public SubTaskWrapperTask(T subTask) {
    this.task = subTask;
  }

  protected T getTask() {
    return task;
  }
}