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

package org.apache.samza.storage.blobstore.index;

import com.google.common.base.Preconditions;

import java.util.List;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Representation of a directory in the blob store
 */
public class DirIndex {
  public static final String ROOT_DIR_NAME = "";
  private static final short SCHEMA_VERSION = 1;

  private final String dirName;

  private final List<FileIndex> filesPresent;
  private final List<FileIndex> filesRemoved;

  // Note: subDirsPresent can also have filesRemoved and subDirectoriesRemoved within them.
  private final List<DirIndex> subDirsPresent;
  private final List<DirIndex> subDirsRemoved;

  public DirIndex(String dirName,
      List<FileIndex> filesPresent, List<FileIndex> filesRemoved,
      List<DirIndex> subDirsPresent, List<DirIndex> subDirsRemoved) {
    Preconditions.checkNotNull(dirName); // may be empty for root dirs
    Preconditions.checkNotNull(filesPresent);
    Preconditions.checkNotNull(filesRemoved);
    Preconditions.checkNotNull(subDirsPresent);
    Preconditions.checkNotNull(subDirsRemoved);
    this.dirName = dirName;
    this.filesPresent = filesPresent;
    this.filesRemoved = filesRemoved;
    this.subDirsPresent = subDirsPresent;
    this.subDirsRemoved = subDirsRemoved;
  }

  public static short getSchemaVersion() {
    return SCHEMA_VERSION;
  }

  public String getDirName() {
    return dirName;
  }

  public List<FileIndex> getFilesPresent() {
    return filesPresent;
  }

  public List<FileIndex> getFilesRemoved() {
    return filesRemoved;
  }

  public List<DirIndex> getSubDirsPresent() {
    return subDirsPresent;
  }

  public List<DirIndex> getSubDirsRemoved() {
    return subDirsRemoved;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    DirIndex that = (DirIndex) o;

    return new EqualsBuilder()
        .append(getDirName(), that.getDirName())
        .append(getFilesPresent(), that.getFilesPresent())
        .append(getFilesRemoved(), that.getFilesRemoved())
        .append(getSubDirsPresent(), that.getSubDirsPresent())
        .append(getSubDirsRemoved(), that.getSubDirsRemoved())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(getDirName()).append(getFilesPresent())
        .append(getFilesRemoved())
        .append(getSubDirsPresent())
        .append(getSubDirsRemoved())
        .toHashCode();
  }

  @Override
  public String toString() {
    return "DirIndex{" + "dirName='" + dirName + '\'' + ", filesPresent=" + filesPresent + ", filesRemoved="
        + filesRemoved + ", subDirsPresent=" + subDirsPresent + ", subDirsRemoved=" + subDirsRemoved + '}';
  }
}
