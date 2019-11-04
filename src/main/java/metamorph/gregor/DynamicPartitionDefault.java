package metamorph.gregor;

import metamorph.cockroach.IDynamicParitions;
import metamorph.cockroach.IManualSinkCommit;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;

public class DynamicPartitionDefault implements IDynamicParitions {

  public void open(Collection<TopicPartition> partitions) {
  }


  public void close(Collection<TopicPartition> partitions) {
  }
}
