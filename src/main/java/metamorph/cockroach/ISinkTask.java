package metamorph.cockroach;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.Collection;
import java.util.Map;

public interface ISinkTask {
  String version();

  void start(Map<String, String> var1);

  void stop();

  public void initialize(SinkTaskContext context);

  public void put(Collection<SinkRecord> var1);




}
