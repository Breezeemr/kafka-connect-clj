package metamorph.gregor;

import metamorph.cockroach.IManualSinkCommit;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public class ManualSinkCommitDefault implements IManualSinkCommit {
  public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition,
      OffsetAndMetadata> currentOffsets) {
      flush(currentOffsets);
      return currentOffsets;
  }


  public void flush(Map<TopicPartition, OffsetAndMetadata> other) {}
}
