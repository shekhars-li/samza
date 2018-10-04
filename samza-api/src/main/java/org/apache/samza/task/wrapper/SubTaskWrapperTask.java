package org.apache.samza.task.wrapper;

/**
 * NOTE: LinkedIn only. This class is the base class to allow wrapper task to be constructed with an {@code subTask} instance
 */
public abstract class SubTaskWrapperTask<T> extends TaskWrapperBase<T> {
  public static final String STREAM_TASK_WRAPPER_CLASS_CFG = "task.streamtaskwrapper.class";
  public static final String ASYNC_STREAM_TASK_WRAPPER_CLASS_CFG = "task.asyncstreamtaskwrapper.class";

  private final T task;

  public SubTaskWrapperTask(T subTask) {
    this.task = subTask;
  }

  protected T getTask() {
    return task;
  }
}