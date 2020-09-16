package com.breezeehr.connect.clj;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SourceConnector extends org.apache.kafka.connect.source.SourceConnector {
    private static final ConfigDef CONFIG_DEF = new ConfigDef().define("task.implfn",
            ConfigDef.Type.STRING,
            ConfigDef.Importance.HIGH,
            "references a clojure var that returns a map of functions for the task implementation");
    private Map<String, String> config;

    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }

    public Class<? extends Task> taskClass() {
        return SourceTask.class;
    }

    public void start(Map<String, String> map) {
        config =map;
    }
    
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        final ArrayList<Map<String, String>> cfgs = new ArrayList<>(1);
        cfgs.add(new HashMap<>(config));
        //only one task for now
        return cfgs;
    }
    
    public void stop() {}
    
    public ConfigDef config() {
        return CONFIG_DEF;
    }    
}
