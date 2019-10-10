package com.redhat.cajun.navy.mission.tracing;

import java.util.List;
import java.util.Map;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Producer;

public class TracingKafkaProducer<K, V> implements KafkaProducer<K, V> {

    private KafkaProducer<K, V> delegate;

    private Tracer tracer;

    private TracingKafkaProducer(KafkaProducer<K, V> producer, Tracer tracer) {
        this.delegate = producer;
        this.tracer = tracer;
    }

    public static <K, V> KafkaProducer<K, V> create(Vertx vertx, Map<String, String> config, Tracer tracer)  {
        KafkaProducer<K, V> producer = KafkaProducer.create(vertx, config);
        return new TracingKafkaProducer<>(producer, tracer);
    }

    @Override
    public KafkaProducer<K, V> exceptionHandler(Handler<Throwable> handler) {
        delegate.exceptionHandler(handler);
        return this;
    }

    @Override
    public KafkaProducer<K, V> write(KafkaProducerRecord<K, V> kafkaProducerRecord) {
        return write(kafkaProducerRecord, null);
    }

    @Override
    public KafkaProducer<K, V> write(KafkaProducerRecord<K, V> record, Handler<AsyncResult<RecordMetadata>> handler) {
        Span span = TracingUtils.buildAndInjectSpan(record, tracer);
        try (Scope ignored = tracer.activateSpan(span)) {
            delegate.write(record, handler);
        } finally {
            span.finish();
        }
        return this;
    }

    @Override
    public void end() {
        delegate.end();
    }

    @Override
    public void end(KafkaProducerRecord<K, V> kafkaProducerRecord) {
        delegate.end(kafkaProducerRecord);
    }

    @Override
    public KafkaProducer<K, V> setWriteQueueMaxSize(int i) {
        delegate.setWriteQueueMaxSize(i);
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return delegate.writeQueueFull();
    }

    @Override
    public KafkaProducer<K, V> drainHandler(Handler<Void> handler) {
        delegate.drainHandler(handler);
        return this;
    }

    @Override
    public KafkaProducer<K, V> partitionsFor(String topic, Handler<AsyncResult<List<PartitionInfo>>> handler) {
        delegate.partitionsFor(topic, handler);
        return this;
    }

    @Override
    public KafkaProducer<K, V> flush(Handler<Void> completionHandler) {
        delegate.flush(completionHandler);
        return this;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void close(Handler<AsyncResult<Void>> completionHandler) {
        delegate.close(completionHandler);
    }

    @Override
    public void close(long timeout, Handler<AsyncResult<Void>> completionHandler) {
        delegate.close(timeout, completionHandler);
    }

    @Override
    public KafkaWriteStream<K, V> asStream() {
        return delegate.asStream();
    }

    @Override
    public Producer<K, V> unwrap() {
        return delegate.unwrap();
    }
}
