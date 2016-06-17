package org.apache.samza.checkpoint;

import org.apache.samza.system.SystemStreamPartition;


/**
 * Convert the given offset to a safe one for checkpointing
 * e.g. Kafka consumer with large message support
 */
public interface CheckpointSafeOffset {
  public String checkpointSafeOffset(SystemStreamPartition ssp, String offset);
}
