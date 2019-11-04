package metamorph.gregor;

import metamorph.cockroach.IManualSourceCommit;
import org.apache.kafka.connect.source.SourceRecord;

public class ManualSourceCommitDefault implements IManualSourceCommit {
  public void commit() throws InterruptedException {}

  public void commitRecord(SourceRecord record) throws InterruptedException {}
}
