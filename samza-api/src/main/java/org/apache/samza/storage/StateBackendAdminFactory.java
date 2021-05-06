package org.apache.samza.storage;

import org.apache.samza.config.Config;
import org.apache.samza.job.model.JobModel;


public interface StateBackendAdminFactory {
  public StateBackendAdmin getStateBackendAdmin(Config config, JobModel jobModel);
}
