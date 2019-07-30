#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either exprFess or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This script is invoked using Java's -XX:OnOutOfMemoryError flag from run-class.sh.
# It copies a job's .metadata files to a samza-admin accessible location on disk.
# Samza-admin then reads it, emits an event to the job's diagnostic stream, and deletes the file.

CONTAINER_METADATA_DIR=$1 # path of the container's metadata file (same as log-dir)
CONTAINER_METADATA_FILEPATH_FOR_OOM=/tmp/samza-container-OOM/ # path to which the metadata needs to be copied in case of OOM

# Create directory if not exists
if [[ ! -d "$CONTAINER_METADATA_FILEPATH_FOR_OOM" ]]; then
  echo "Creating dir" $CONTAINER_METADATA_FILEPATH_FOR_OOM
  mkdir -p $CONTAINER_METADATA_FILEPATH_FOR_OOM
  echo "Created dir" $CONTAINER_METADATA_FILEPATH_FOR_OOM

  # Setting the right permissions
  echo "Setting permissions on $CONTAINER_METADATA_FILEPATH_FOR_OOM to 777"
  chmod 777 $CONTAINER_METADATA_FILEPATH_FOR_OOM
else
  echo "Dir" $CONTAINER_METADATA_FILEPATH_FOR_OOM "already exists."
fi

echo "Looking for metadata files in $CONTAINER_METADATA_DIR"
# Check if metadata file exists in dir
if ls $CONTAINER_METADATA_DIR/*.metadata 1> /dev/null 2>&1; then

    echo "Copying metadata files to " $CONTAINER_METADATA_FILEPATH_FOR_OOM

    # Copy the container's metadata to the OOM directory
    for metadataFile in $CONTAINER_METADATA_DIR/*.metadata;
      do
         cp "$metadataFile" "$CONTAINER_METADATA_FILEPATH_FOR_OOM"
         echo "Copied" $metadataFile "to" $CONTAINER_METADATA_FILEPATH_FOR_OOM
         chmod -R 777 $CONTAINER_METADATA_FILEPATH_FOR_OOM
      done

else
    echo "No metadata file found for copying to $CONTAINER_METADATA_FILEPATH_FOR_OOM"
fi
