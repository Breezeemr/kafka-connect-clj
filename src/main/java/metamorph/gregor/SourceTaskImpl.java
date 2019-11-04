package metamorph.gregor;

import clojure.lang.IFn;
import com.breezeehr.connect.clj.CljRequirer;
import metamorph.cockroach.IManualSourceCommit;
import metamorph.cockroach.ISourceTask;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SourceTaskImpl extends SourceTask {
  private ISourceTask impl;
  private IManualSourceCommit commitimpl;

  public String version() {
    return getClass().getPackage().getImplementationVersion();
  }

  public void start(Map<String, String> config) {
    String override = config.get("task.impl");
    if (override != null) {
      IFn overridefn = CljRequirer.getVar(override);
      Object implpprecheck = overridefn.invoke(config);
      if (implpprecheck instanceof ISourceTask) {
        impl = (ISourceTask) implpprecheck;
      } else {
        //handle error
      }
      if (implpprecheck instanceof IManualSourceCommit) {
        commitimpl = (IManualSourceCommit) implpprecheck;
      } else {
        commitimpl = new ManualSourceCommitDefault();
      }
    }
    if (impl != null) {
      impl.start(config);
    }
  }

  @Override
  public void initialize(SourceTaskContext context) {
    super.initialize(context);
    if (impl != null) {
      impl.initialize(context);
    }
  }

  public List<SourceRecord> poll() throws InterruptedException {
    if (impl != null) {
      return impl.poll();
    }
    return new ArrayList<SourceRecord>();
  }


  public synchronized void stop() {
    if (impl != null) {
      impl.stop();
    }
    impl = null;
  }

  public void commit() throws InterruptedException {
    commitimpl.commit();
  }

  public void commitRecord(SourceRecord record) throws InterruptedException {
    commitimpl.commitRecord(record);
  }
}
