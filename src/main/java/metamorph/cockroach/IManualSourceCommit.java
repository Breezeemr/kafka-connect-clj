package metamorph.cockroach;

import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

public interface IManualSourceCommit {

  public void commit() throws InterruptedException;

  public void commitRecord(SourceRecord record) throws InterruptedException;
}
