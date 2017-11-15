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

import static com.breezeehr.connect.clj.CljRequirer.*;

public class CljSinkTask extends SinkTask {
    private IFn putFn;
    private IFn stopFn;
    private IFn flushFn;

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
        IFn startFn = getFN(m, "start");
        putFn = getFN(m,"put" );
        assert null !=putFn;
        stopFn = getFN(m,"stop" );
        flushFn = getFN(m, "flush");
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



}
 
