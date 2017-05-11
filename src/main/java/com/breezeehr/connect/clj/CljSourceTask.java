package com.breezeehr.connect.clj.cljSourceTask;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.List;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;

public class CljSourceTask extends SourceTask {

    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }

    public void start(Map<String, String> props) {
    }

    public List<SourceRecord> poll() throws InterruptedException {
	return null;
    }

    public synchronized void stop() {
    }
}
