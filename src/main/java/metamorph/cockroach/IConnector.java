package metamorph.cockroach;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectorContext;

import java.util.List;
import java.util.Map;

public interface IConnector {
  public void initialize(ConnectorContext ctx);

  public void initialize(ConnectorContext ctx, List<Map<String, String>> taskConfigs);

  public void start(Map<String, String> var1);

  public void reconfigure(Map<String, String> props);

  public List<Map<String, String>> taskConfigs(int var1);

  public void stop();

  public Config validate(Map<String, String> connectorConfigs) ;

  public ConfigDef config();
}
