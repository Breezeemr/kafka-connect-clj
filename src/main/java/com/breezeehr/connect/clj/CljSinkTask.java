package com.breezeehr.connect.clj;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import java.util.NoSuchElementException;
    
import java.util.List;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;

import clojure.lang.IFn;
import clojure.java.api.Clojure;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class CljSinkTask extends SinkTask {
    private IFn putFn;
    private IFn stopFn;
    private IFn flushFn;
    private static IFn REQUIRE = Clojure.var("clojure.core", "require");
    private static IFn SYMBOL = Clojure.var("clojure.core", "symbol");
    private static IFn KEYWORD = Clojure.var("clojure.core", "keyword");
    private static IFn DEREF = Clojure.var("clojure.core", "deref");

    public Object state;

    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }

    public SinkTaskContext getContext (){
        return context;
    }
    
    public void start(Map<String, String> config) {
        Map m = getVar(config);
        assert m != null;
        IFn startFn = getFN(m, KEYWORD.invoke("start"));
        putFn = getFN(m,KEYWORD.invoke("put") );
        assert null !=putFn;
        stopFn = getFN(m,KEYWORD.invoke("stop") );
        flushFn = getFN(m, KEYWORD.invoke("flush"));
        if (putFn == null) {
            throw new NoSuchElementException("Missing required parameter 'service'");
        }
        if (startFn != null) { state = startFn.invoke(this, config); }

    }

    public void put(Collection<SinkRecord> records) {
	putFn.invoke(this, records);
    }

    public void flush(Map<TopicPartition, OffsetAndMetadata> other) {
        if (flushFn != null) { flushFn.invoke(this, other); }
    }

    public synchronized void stop() {
	if (stopFn != null) {
            stopFn.invoke(this);
        }
	state = null;
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
        if (varName == null) {
            throw new NoSuchElementException("Must provide reference to implementation at config key 'clj.impl'" );
        }

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

        Object item = DEREF.invoke(Clojure.var(namespace, name));
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
 
