package metamorph.gregor;

import clojure.lang.IFn;
import com.breezeehr.connect.clj.CljRequirer;
import metamorph.cockroach.IDynamicParitions;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import metamorph.cockroach.ISinkTask;

import java.util.Collection;
import java.util.Map;

public class SinkTaskImpl extends SinkTask {
  private ISinkTask impl;
  private ManualSinkCommitDefault sinkcommitimpl;
  private IDynamicParitions dynpartimpl;
  public String version() {
    return getClass().getPackage().getImplementationVersion();
  }


  public void start(Map<String, String> config) {
    String override = config.get("task.impl");
    if (override != null) {
      IFn overridefn = CljRequirer.getVar(override);
      Object implpprecheck = overridefn.invoke(config);
      if (implpprecheck instanceof ISinkTask) {
        impl = (ISinkTask) implpprecheck;
      } else {
        //handle error
      }
      if (implpprecheck instanceof ManualSinkCommitDefault){
        sinkcommitimpl= (ManualSinkCommitDefault)implpprecheck;
      }else{
        sinkcommitimpl= new ManualSinkCommitDefault();
      }
      if (implpprecheck instanceof IDynamicParitions){
        dynpartimpl= (IDynamicParitions)implpprecheck;
      }else{
        dynpartimpl= new DynamicPartitionDefault();
      }
    }
    impl.start(config);

  }

  @Override
  public void initialize(SinkTaskContext context) {
    impl.initialize(context);
  }

  public void put(Collection<SinkRecord> records) {
    impl.put(records);
  }

  public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition,
      OffsetAndMetadata> currentOffsets) {
    return sinkcommitimpl.preCommit(currentOffsets);
  }


  public void flush(Map<TopicPartition, OffsetAndMetadata> other) {
    sinkcommitimpl.flush(other);

  }

  public synchronized void stop() {
    impl.stop();
    impl = null;
    sinkcommitimpl= null;
    dynpartimpl =null;

  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    dynpartimpl.open(partitions);
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    dynpartimpl.close(partitions);
  }
}
 
