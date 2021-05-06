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

package org.apache.samza.storage.blobstore;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;


public class Metadata {
  private final String payloadPath;
  private final String payloadSize;
  private final String jobName;
  private final String jobId;
  private final String taskName;
  private final String storeName;

  public Metadata(String payloadPath, String payloadSize,
      String jobName, String jobId, String taskName, String storeName) {
    this.payloadPath = payloadPath;
    this.payloadSize = payloadSize;
    this.jobName = jobName;
    this.jobId = jobId;
    this.taskName = taskName;
    this.storeName = storeName;
  }

  public String getPayloadPath() {
    return payloadPath;
  }

  public String getPayloadSize() {
    return payloadSize;
  }

  public String getJobName() {
    return jobName;
  }

  public String getJobId() {
    return jobId;
  }

  public String getTaskName() {
    return taskName;
  }

  public String getStoreName() {
    return storeName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof Metadata)) {
      return false;
    }

    Metadata that = (Metadata) o;

    return new EqualsBuilder().append(getPayloadPath(), that.getPayloadPath())
        .append(getPayloadSize(), that.getPayloadSize())
        .append(getJobName(), that.getJobName())
        .append(getJobId(), that.getJobId())
        .append(getTaskName(), that.getTaskName())
        .append(getStoreName(), that.getStoreName())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(getPayloadPath())
        .append(getPayloadSize())
        .append(getJobName())
        .append(getJobId())
        .append(getTaskName())
        .append(getStoreName())
        .toHashCode();
  }

  @Override
  public String toString() {
    return "Metadata{" + "payloadPath='" + payloadPath + '\'' + ", payloadSize='" + payloadSize + '\''
        + ", jobName='" + jobName + '\'' + ", jobId='" + jobId + '\'' + ", taskName='" + taskName + '\''
        + ", storeName='" + storeName + '\'' + '}';
  }
}
