package com.breezeehr.connect.clj;

import clojure.lang.IFn;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.breezeehr.connect.clj.CljRequirer.*;

public class SourceTask extends org.apache.kafka.connect.source.SourceTask {
    private IFn pollFn;
    private IFn stopFn;
    private IFn reinit;
    private IFn commitFn;
    private IFn commitRecordFn;

    public SourceTaskContext getContext(){
      return context;
    }

    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }

    public void start(Map<String, String> config) {
        if(reinit==null) {
            String implFNVar = config.get("task.implfn");
            if (implFNVar != null) {
                IFn implFN = getStaticFN(implFNVar);
                Object ret = implFN.invoke(config,context);
                if (ret instanceof Map) {
                    Map m = (Map) ret;
                    reinit = getFN(m, "reinit");
                    pollFn = getFN(m, "poll");
                    assert null != pollFn;
                    stopFn = getFN(m, "stop");
                    commitFn = getFN(m, "commit");
                    commitRecordFn = getFN(m,"commitRecord");
                    if (pollFn == null) {
                        throw new NoSuchElementException("Missing required parameter 'service'");
                    }
                } else {
                    throw new NoSuchElementException("task.implfn does not return map");
                }
            } else {
                throw new NoSuchElementException("Missing required parameter 'task.implfn'");
            }
        }else{
            reinit.invoke(config,context);
        }
    }

    public List<SourceRecord> poll() throws InterruptedException {
	return (List<SourceRecord>) pollFn.invoke();
    }
    
    
    public synchronized void stop() {
	if (stopFn != null) {
            stopFn.invoke();
        }
    }
    public void commit() throws InterruptedException {
        if (commitFn != null) {
            commitFn.invoke();
        }
    }
    public void commitRecord(SourceRecord record, RecordMetadata metadata) throws InterruptedException {
        if (commitRecordFn != null) {
            commitRecordFn.invoke(record,metadata);
        }
    }
}
