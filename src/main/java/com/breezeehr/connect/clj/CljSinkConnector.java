package com.breezeehr.connect.clj;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CljSinkConnector extends SinkConnector {
    private static final ConfigDef CONFIG_DEF = new ConfigDef();
    private Map<String, String> config;

    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }

    public Class<? extends Task> taskClass() {
        return CljSinkTask.class;
    }

    public void start(Map<String, String> map) {
        final String[] setting_keys = {};
        config = new HashMap<>(setting_keys.length);
        for (String k : setting_keys) {
            config.put(k, map.get(k));
        }
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
    }
    
    public ConfigDef config() {
        return CONFIG_DEF;
    }    
    // public static void main( String[] args ) {
    //     System.out.println( "Hello World!" );
    // }
}
