package metamorph.cockroach;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public interface IDynamicParitions {
  public void open(Collection<TopicPartition> partitions);
  public void close(Collection<TopicPartition> partitions);
}
