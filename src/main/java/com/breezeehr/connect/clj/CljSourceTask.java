package com.breezeehr.connect.clj;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import java.util.NoSuchElementException;

import java.util.List;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;

import clojure.lang.IFn;
import clojure.java.api.Clojure;

public class CljSourceTask extends SourceTask {
    private IFn pollFn;
    private IFn stopFn;
    private IFn flushFn;
    private static IFn REQUIRE = Clojure.var("clojure.core", "require");
    private static IFn SYMBOL = Clojure.var("clojure.core", "symbol");

    public Object state;

    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }

    public void start(Map<String, String> config) {
        IFn startFn = getVar(config, "clj.start");
        pollFn = getVar(config, "clj.poll");
        stopFn = getVar(config, "clj.stop");
    	flushFn = getVar(config, "clj.flush");
	if (pollFn == null) {
            throw new NoSuchElementException("Missing required parameter 'service'");
        }

        if (startFn != null) { state = startFn.invoke(this, config); }
    }

    public List<SourceRecord> poll() throws InterruptedException {
	return (List<SourceRecord>) pollFn.invoke(this);
    }
    
    
    public synchronized void stop() {
	if (stopFn != null) {
            stopFn.invoke(this);
        }
	state = null;
    }

    private static IFn getVar(Map<String, String> config, String param)
        throws NoSuchElementException {

        String varName = config.get(param);
        if (varName == null) { return null; }

        String[] parts = varName.split("/", 2);
        String namespace = parts[0];
        String name = parts[1];
        if (namespace == null || name == null) {
            throw new NoSuchElementException("Invalid namespace-qualified symbol '" + varName + "'");
        }

        try {
            REQUIRE.invoke(SYMBOL.invoke(namespace));
        } catch(Throwable t) {
            throw new NoSuchElementException("Failed to load namespace '" + namespace + "'");
        }

        IFn fn = Clojure.var(namespace, name);
        if (fn == null) {
            throw new NoSuchElementException("Var '" + varName + "' not found");
        }
        return fn;
    }
}
