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
import org.apache.kafka.connect.source.SourceTaskContext;

public class CljSourceTask extends SourceTask {
    private IFn pollFn;
    private IFn stopFn;
    private IFn commitFn;
    private IFn commitRecordFn;
    private static IFn REQUIRE = Clojure.var("clojure.core", "require");
    private static IFn SYMBOL = Clojure.var("clojure.core", "symbol");
    private static IFn KEYWORD = Clojure.var("clojure.core", "keyword");

    public Object state;

    public SourceTaskContext getContext( ){
      return context;
    }

    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }

    public void start(Map<String, String> config) {
        Map m = getVar(config);
        if (m != null){
            IFn startFn = getFN(m, KEYWORD.invoke("start"));
            pollFn = getFN(m, KEYWORD.invoke("poll"));
            stopFn = getFN(m, KEYWORD.invoke("stop"));
            commitFn = getFN(m, KEYWORD.invoke("commit"));
            commitRecordFn = getFN(m,KEYWORD.invoke("commitRecord"));
            if (pollFn == null) {
                throw new NoSuchElementException("Missing required parameter 'service'");
            }
            if (startFn != null) { state = startFn.invoke(this, config); }
        }
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
    public void commit() throws InterruptedException {
        if (commitFn != null) {
            commitFn.invoke(this);
        }
    }
    public void commitRecord(SourceRecord record) throws InterruptedException {
        if (commitRecordFn != null) {
            commitRecordFn.invoke(this, record);
        }
    }
    private static IFn getFN (Map m, Object param) throws NoSuchElementException {
        Object item = m.get(param);
        if (item instanceof IFn ){
            return (IFn) item;
        }
        return null;
    }
    private static Map getVar(Map<String, String> config)
        throws NoSuchElementException {

        String varName = config.get("clj.impl");
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

        Object item = Clojure.var(namespace, name);
        if (item == null) {
            throw new NoSuchElementException("Var '" + varName + "' not found");
        }
        if (item instanceof Map) {
            return ( Map) item;
        } else {
            throw new NoSuchElementException("value at cljs.impl is not a map.");
        }
    }
}
