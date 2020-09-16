package com.breezeehr.connect.clj;

import clojure.lang.IFn;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.breezeehr.connect.clj.CljRequirer.*;

public class SinkTask extends org.apache.kafka.connect.sink.SinkTask {
    private IFn putFn;
    private IFn stopFn;
    private IFn reinit;
    private IFn flushFn;
    private IFn preCommitFn;

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
                    putFn = getFN(m, "put");
                    assert null != putFn;
                    stopFn = getFN(m, "stop");
                    flushFn = getFN(m, "flush");
                    preCommitFn = getFN(m, "preCommit");
                    if (putFn == null) {
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

    public void put(Collection<SinkRecord> records) {
        putFn.invoke(records);
    }

    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition,
            OffsetAndMetadata> currentOffsets) {
        Map<TopicPartition,
                OffsetAndMetadata> updatedOffsets;
        if (preCommitFn != null) {
            Object temp = preCommitFn.invoke(currentOffsets);
            if (temp instanceof Map) {
                updatedOffsets = (Map) temp;
            } else {
                updatedOffsets = currentOffsets;
            }
        } else {
            updatedOffsets = currentOffsets;
        }
        flush(updatedOffsets);
        return updatedOffsets;
    }

    public void flush(Map<TopicPartition, OffsetAndMetadata> other) {
        if (flushFn != null) {
            flushFn.invoke(other);
        }
    }

    public synchronized void stop() {
        if (stopFn != null) {
            stopFn.invoke();
        }
    }


}
 
