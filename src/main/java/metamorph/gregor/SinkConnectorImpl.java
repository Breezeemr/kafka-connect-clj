package metamorph.gregor;

import clojure.lang.IFn;
import com.breezeehr.connect.clj.CljRequirer;
import metamorph.cockroach.IConnector;
import metamorph.gregor.SinkTaskImpl;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SinkConnectorImpl extends org.apache.kafka.connect.sink.SinkConnector {
  private IConnector impl;
  private Map<String, String> config;

  /**
   * Initialize this connector, using the provided ConnectorContext to notify the runtime of
   * input configuration changes.
   *
   * @param ctx context object used to interact with the Kafka Connect runtime
   */
  public void initialize(ConnectorContext ctx) {
    context = ctx;
  }

  public String version() {
    return getClass().getPackage().getImplementationVersion();
  }

  public Class<? extends Task> taskClass() {
    return SinkTaskImpl.class;
  }

  public void start(Map<String, String> map) {
    config = map;
    String override = config.get("connector.override");
    if (override != null) {
      IFn overridefn = CljRequirer.getVar(override);
      Object implpprecheck = overridefn.invoke(map);
      if (implpprecheck instanceof IConnector) {
        impl = (IConnector) implpprecheck;
      } else {
        context.raiseError(new Exception("tasktype is not instance of IConnector"));
      }
    }
    if (impl != null) {
      impl.start(config);
    }
  }

  public List<Map<String, String>> taskConfigs(int maxTasks) {
    if (impl != null) {
      return impl.taskConfigs(maxTasks);
    } else {
      final ArrayList<Map<String, String>> cfgs = new ArrayList<>(maxTasks);
      for (int i = 0; i < maxTasks; i++) {
        cfgs.add(new HashMap<>(config));
      }
      return cfgs;
    }
  }

  public void stop() {
    if (impl != null) {
      impl.stop();
    }
    config = null;
    impl = null;
  }

  public ConfigDef config() {
    return new ConfigDef();
  }
}
