package com.redhat.cajun.navy.mission.tracing;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.opentracing.Tracer;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.ReadStream;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import io.vertx.kafka.client.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.Consumer;

public class TracingKafkaConsumer<K, V> implements KafkaConsumer<K, V> {

    private KafkaConsumer<K, V> delegate;

    private Tracer tracer;

    private TracingKafkaConsumer(KafkaConsumer<K, V> consumer, Tracer tracer) {
        this.delegate = consumer;
        this.tracer = tracer;
    }

    public static <K, V> KafkaConsumer<K, V> create(Vertx vertx, Map<String, String> config, Tracer tracer) {
        KafkaConsumer<K, V> consumer = KafkaConsumer.create(vertx, config);
        return new TracingKafkaConsumer<>(consumer, tracer);
    }

    @Override
    public KafkaConsumer<K, V> exceptionHandler(Handler<Throwable> handler) {
        delegate.exceptionHandler(handler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> handler(Handler<KafkaConsumerRecord<K, V>> handler) {
        Handler<KafkaConsumerRecord<K, V>> decorated = event -> {
            TracingUtils.buildAndFinishChildSpan(event, tracer);
            handler.handle(event);
        };
        delegate.handler(decorated);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> pause() {
        delegate.pause();
        return this;
    }

    @Override
    public KafkaConsumer<K, V> resume() {
        delegate.resume();
        return this;
    }

    @Override
    public ReadStream<KafkaConsumerRecord<K, V>> fetch(long amount) {
        delegate.fetch(amount);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> endHandler(Handler<Void> endHandler) {
        delegate.endHandler(endHandler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> subscribe(String topic) {
        return this.subscribe(Collections.singleton(topic));
    }

    @Override
    public KafkaConsumer<K, V> subscribe(Set<String> topics) {
        return this.subscribe(topics, null);
    }

    @Override
    public KafkaConsumer<K, V> subscribe(String topic, Handler<AsyncResult<Void>> completionHandler) {
        return this.subscribe(Collections.singleton(topic), completionHandler);
    }

    @Override
    public KafkaConsumer<K, V> subscribe(Set<String> topics, Handler<AsyncResult<Void>> completionHandler) {
        delegate.subscribe(topics, completionHandler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> assign(TopicPartition topicPartition) {
        return this.assign(Collections.singleton(topicPartition));
    }

    @Override
    public KafkaConsumer<K, V> assign(Set<TopicPartition> topicPartitions) {
        return this.assign(topicPartitions, null);
    }

    @Override
    public KafkaConsumer<K, V> assign(TopicPartition topicPartition, Handler<AsyncResult<Void>> completionHandler) {
        return this.assign(Collections.singleton(topicPartition), completionHandler);
    }

    @Override
    public KafkaConsumer<K, V> assign(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
        delegate.assign(topicPartitions, completionHandler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> assignment(Handler<AsyncResult<Set<TopicPartition>>> handler) {
        delegate.assignment(handler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> listTopics(Handler<AsyncResult<Map<String, List<PartitionInfo>>>> handler) {
        delegate.listTopics(handler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> unsubscribe() {
        return this.unsubscribe(null);
    }

    @Override
    public KafkaConsumer<K, V> unsubscribe(Handler<AsyncResult<Void>> completionHandler) {
        delegate.unsubscribe(completionHandler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> subscription(Handler<AsyncResult<Set<String>>> handler) {
        delegate.subscription(handler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> pause(TopicPartition topicPartition) {
        return this.pause(Collections.singleton(topicPartition));
    }

    @Override
    public KafkaConsumer<K, V> pause(Set<TopicPartition> topicPartitions) {
        return this.pause(topicPartitions, null);
    }

    @Override
    public KafkaConsumer<K, V> pause(TopicPartition topicPartition, Handler<AsyncResult<Void>> completionHandler) {
        return this.pause(Collections.singleton(topicPartition), completionHandler);
    }

    @Override
    public KafkaConsumer<K, V> pause(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
        delegate.pause(topicPartitions, completionHandler);
        return this;
    }

    @Override
    public void paused(Handler<AsyncResult<Set<TopicPartition>>> handler) {
        delegate.paused(handler);
    }

    @Override
    public KafkaConsumer<K, V> resume(TopicPartition topicPartition) {
        return this.resume(Collections.singleton(topicPartition));
    }

    @Override
    public KafkaConsumer<K, V> resume(Set<TopicPartition> topicPartitions) {
        return this.resume(topicPartitions, null);
    }

    @Override
    public KafkaConsumer<K, V> resume(TopicPartition topicPartition, Handler<AsyncResult<Void>> completionHandler) {
        return this.resume(Collections.singleton(topicPartition), completionHandler);
    }

    @Override
    public KafkaConsumer<K, V> resume(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
        delegate.resume(topicPartitions, completionHandler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> partitionsRevokedHandler(Handler<Set<TopicPartition>> handler) {
        delegate.partitionsRevokedHandler(handler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> partitionsAssignedHandler(Handler<Set<TopicPartition>> handler) {
        delegate.partitionsAssignedHandler(handler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> seek(TopicPartition topicPartition, long offset) {
        return this.seek(topicPartition, offset, null);
    }

    @Override
    public KafkaConsumer<K, V> seek(TopicPartition topicPartition, long offset, Handler<AsyncResult<Void>> completionHandler) {
        delegate.seek(topicPartition, offset, completionHandler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> seekToBeginning(TopicPartition topicPartition) {
        return this.seekToBeginning(Collections.singleton(topicPartition));
    }

    @Override
    public KafkaConsumer<K, V> seekToBeginning(Set<TopicPartition> topicPartitions) {
        return this.seekToBeginning(topicPartitions, null);
    }

    @Override
    public KafkaConsumer<K, V> seekToBeginning(TopicPartition topicPartition, Handler<AsyncResult<Void>> completionHandler) {
        return this.seekToBeginning(Collections.singleton(topicPartition), completionHandler);
    }

    @Override
    public KafkaConsumer<K, V> seekToBeginning(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
        delegate.seekToBeginning(topicPartitions, completionHandler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> seekToEnd(TopicPartition topicPartition) {
        return this.seekToEnd(Collections.singleton(topicPartition));
    }

    @Override
    public KafkaConsumer<K, V> seekToEnd(Set<TopicPartition> topicPartitions) {
        return this.seekToEnd(topicPartitions, null);
    }

    @Override
    public KafkaConsumer<K, V> seekToEnd(TopicPartition topicPartition, Handler<AsyncResult<Void>> completionHandler) {
        return this.seekToEnd(Collections.singleton(topicPartition), completionHandler);
    }

    @Override
    public KafkaConsumer<K, V> seekToEnd(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Void>> completionHandler) {
        delegate.seekToEnd(topicPartitions, completionHandler);
        return this;
    }

    @Override
    public void commit() {
        delegate.commit();
    }

    @Override
    public void commit(Handler<AsyncResult<Void>> completionHandler) {
        delegate.commit(completionHandler);
    }

    @Override
    public void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        delegate.commit(offsets);
    }

    @Override
    public void commit(Map<TopicPartition, OffsetAndMetadata> offsets, Handler<AsyncResult<Map<TopicPartition, OffsetAndMetadata>>> completionHandler) {
        delegate.commit(offsets, completionHandler);
    }

    @Override
    public void committed(TopicPartition topicPartition, Handler<AsyncResult<OffsetAndMetadata>> handler) {
        delegate.committed(topicPartition, handler);
    }

    @Override
    public KafkaConsumer<K, V> partitionsFor(String topic, Handler<AsyncResult<List<PartitionInfo>>> handler) {
        delegate.partitionsFor(topic, handler);
        return this;
    }

    @Override
    public KafkaConsumer<K, V> batchHandler(Handler<KafkaConsumerRecords<K, V>> handler) {
        delegate.batchHandler(handler);
        return null;
    }

    @Override
    public void close(Handler<AsyncResult<Void>> completionHandler) {
        delegate.close(completionHandler);
    }

    @Override
    public void position(TopicPartition partition, Handler<AsyncResult<Long>> handler) {
        delegate.position(partition, handler);
    }

    @Override
    public void offsetsForTimes(Map<TopicPartition, Long> topicPartitionTimestamps, Handler<AsyncResult<Map<TopicPartition, OffsetAndTimestamp>>> handler) {
        delegate.offsetsForTimes(topicPartitionTimestamps, handler);
    }

    @Override
    public void offsetsForTimes(TopicPartition topicPartition, Long timestamp, Handler<AsyncResult<OffsetAndTimestamp>> handler) {
        delegate.offsetsForTimes(topicPartition, timestamp, handler);
    }

    @Override
    public void beginningOffsets(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Map<TopicPartition, Long>>> handler) {
        delegate.beginningOffsets(topicPartitions, handler);
    }

    @Override
    public void beginningOffsets(TopicPartition topicPartition, Handler<AsyncResult<Long>> handler) {
        delegate.beginningOffsets(topicPartition, handler);
    }

    @Override
    public void endOffsets(Set<TopicPartition> topicPartitions, Handler<AsyncResult<Map<TopicPartition, Long>>> handler) {
        delegate.endOffsets(topicPartitions, handler);
    }

    @Override
    public void endOffsets(TopicPartition topicPartition, Handler<AsyncResult<Long>> handler) {
        delegate.endOffsets(topicPartition, handler);
    }

    @Override
    public KafkaReadStream<K, V> asStream() {
        return delegate.asStream();
    }

    @Override
    public Consumer<K, V> unwrap() {
        return delegate.unwrap();
    }

    @Override
    public KafkaConsumer<K, V> pollTimeout(long timeout) {
        delegate.pollTimeout(timeout);
        return this;
    }

    @Override
    public void poll(long timeout, Handler<AsyncResult<KafkaConsumerRecords<K, V>>> handler) {
        delegate.poll(timeout, handler);
    }
}
