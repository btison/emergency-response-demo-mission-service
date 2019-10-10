package com.redhat.cajun.navy.mission.tracing;


import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import io.opentracing.propagation.TextMap;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.impl.KafkaHeaderImpl;

public class KafkaHeadersInjectAdapter implements TextMap {

    private final List<KafkaHeader> headers;

    KafkaHeadersInjectAdapter(List<KafkaHeader> headers) {
        this.headers = headers;
    }

    @Override
    public Iterator<Entry<String, String>> iterator() {
        throw new UnsupportedOperationException("iterator should never be used with Tracer.inject()");
    }

    @Override
    public void put(String key, String value) {
        headers.add(new KafkaHeaderImpl(key, value));
    }
}
