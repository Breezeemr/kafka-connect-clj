package com.breezeehr.connect.clj;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import java.util.NoSuchElementException;

import java.util.Collection;
import java.util.Map;

import clojure.lang.IFn;
import org.apache.kafka.connect.sink.SinkTaskContext;

import static com.breezeehr.connect.clj.CljRequirer.*;

public class CljSinkTask extends SinkTask {
    private IFn putFn;
    private IFn stopFn;
    private IFn flushFn;
    private IFn preCommitFn;

    public Object state;

    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }

    public SinkTaskContext getContext (){
        return context;
    }
    
    public void start(Map<String, String> config) {
        Map m = getMapVar(config);
        assert m != null;
        IFn startFn = getFN(m, "start");
        putFn = getFN(m,"put" );
        assert null !=putFn;
        stopFn = getFN(m,"stop" );
        flushFn = getFN(m, "flush");
        preCommitFn = getFN(m, "preCommit");
        if (putFn == null) {
            throw new NoSuchElementException("Missing required parameter 'service'");
        }
        if (startFn != null) { state = startFn.invoke(this, config); }

    }

    public void put(Collection<SinkRecord> records) {
	putFn.invoke(this, records);
    }

    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition,
        OffsetAndMetadata> currentOffsets) {
      Map<TopicPartition,
          OffsetAndMetadata> updatedOffsets;
      if (preCommitFn != null) {
        Object temp =preCommitFn.invoke(this, currentOffsets);
        if (temp instanceof Map){
          updatedOffsets=(Map) temp;
        }else {
          updatedOffsets=currentOffsets;
        }
      } else{
        updatedOffsets=currentOffsets;
      }
        flush(updatedOffsets);
        return updatedOffsets;
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
 
