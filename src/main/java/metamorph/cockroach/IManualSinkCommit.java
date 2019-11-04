package metamorph.cockroach;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public interface IManualSinkCommit {
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets);

  public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition,
      OffsetAndMetadata> currentOffsets);
}
