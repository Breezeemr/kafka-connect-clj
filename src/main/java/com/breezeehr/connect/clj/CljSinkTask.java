package com.breezeehr.connect.clj;

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

public class CljSinkTask extends SinkTask {
    private IFn serviceFn;
    private IFn destroyFn;
    private static IFn REQUIRE = Clojure.var("clojure.core", "require");
    private static IFn SYMBOL = Clojure.var("clojure.core", "symbol");

    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }

    // @Override
    // public void init() {
    //     // Map config = new HashMap<String, String>();; 
    //     // IFn initFn = getVar(config, "init");
    //     // serviceFn = getVar(config, "service");
    //     // destroyFn = getVar(config, "destroy");

    //     // if (serviceFn == null) {
    //     //     throw new RetriableException("Missing required parameter 'service'");
    //     // }

    //     // if (initFn != null) { initFn.invoke(this, config); }
    // }
    
    public void start(Map<String, String> config) {
        IFn initFn = getVar(config, "init");
        serviceFn = getVar(config, "service");
        destroyFn = getVar(config, "destroy");
        if (serviceFn == null) {
            throw new NoSuchElementException("Missing required parameter 'service'");
        }

        if (initFn != null) { initFn.invoke(this, config); }
	
    }

    public void put(Collection<SinkRecord> records) {
	serviceFn.invoke(this, records);
    }

    public synchronized void stop() {
	if (destroyFn != null) {
            destroyFn.invoke(this);
        }

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
