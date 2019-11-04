package com.breezeehr.connect.clj;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.NoSuchElementException;

import java.util.List;
import java.util.Map;

import clojure.lang.IFn;
import org.apache.kafka.connect.source.SourceTaskContext;

import static com.breezeehr.connect.clj.CljRequirer.*;

public class CljSourceTask extends SourceTask {
    private IFn pollFn;
    private IFn stopFn;
    private IFn commitFn;
    private IFn commitRecordFn;

    public Object state;

    public SourceTaskContext getContext(){
      return context;
    }

    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }

    public void start(Map<String, String> config) {
        Map m = getMapVar(config);
        assert null != m;

        IFn startFn = getFN(m, "start" );
        pollFn = getFN(m, "poll");
        assert null != pollFn;
        stopFn = getFN(m, "stop");
        commitFn = getFN(m, "commit");
        commitRecordFn = getFN(m,"commitRecord");
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
}
