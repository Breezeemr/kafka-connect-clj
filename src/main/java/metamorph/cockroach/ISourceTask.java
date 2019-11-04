package metamorph.cockroach;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.util.List;
import java.util.Map;

public interface ISourceTask {
  String version();

  void start(Map<String, String> var1);

  void stop();

  public void initialize(SourceTaskContext context);

  public List<SourceRecord> poll() throws InterruptedException;

}
