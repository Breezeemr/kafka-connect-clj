package com.breezeehr.connect.clj;

import clojure.lang.IFn;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SinkConnector extends org.apache.kafka.connect.sink.SinkConnector {
  private static final ConfigDef CONFIG_DEF = new ConfigDef();
  private Class<? extends SinkTask> deft;
  private Map<String, String> config;
  /**
   * Initialize this connector, using the provided ConnectorContext to notify the runtime of
   * input configuration changes.
   * @param ctx context object used to interact with the Kafka Connect runtime
   */
  public void initialize(ConnectorContext ctx) {
    context = ctx;
  }
  public String version() {
    return getClass().getPackage().getImplementationVersion();
  }

  public Class<? extends Task> taskClass() {
    return deft;
  }

  public void start(Map<String, String> map) {
    Object temp = CljRequirer.REQUIRING_RESOLVE.invoke(CljRequirer.SYMBOL.invoke("tasktype"));
    if(temp instanceof IFn){
      Object temp2 = ((IFn) temp).invoke();
      if (temp2 instanceof  SinkTask){
        deft = (Class<? extends SinkTask>) temp2.getClass();

      }else{
        context.raiseError(new Exception("tasktype is not instance of SinkTask"));
      }

    }else{
      context.raiseError(new Exception("cannot resolve tasktype to zero arg fn"));
    }
    config = map;


  }

  public List<Map<String, String>> taskConfigs(int maxTasks) {
    final ArrayList<Map<String, String>> cfgs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; i++) {
      cfgs.add(new HashMap<>(config));
    }
    return cfgs;
  }

  public void stop() {
    config = null;
    deft=null;
  }

  public ConfigDef config() {
    return CONFIG_DEF;
  }
}
