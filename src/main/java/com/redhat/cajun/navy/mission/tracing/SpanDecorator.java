package com.redhat.cajun.navy.mission.tracing;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import io.opentracing.Span;
import io.opentracing.tag.Tags;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducerRecord;


class SpanDecorator {

    static final String COMPONENT_NAME = "vertx-kafka";
    static final String KAFKA_SERVICE = "kafka";

    /**
     * Called before record is sent by producer
     */
    static <K, V> void onSend(KafkaProducerRecord<K, V> record, Span span) {
        setCommonTags(span);
        Tags.MESSAGE_BUS_DESTINATION.set(span, record.topic());
        if (record.partition() != null) {
            span.setTag("partition", record.partition());
        }
    }

    /**
     * Called when record is received in consumer
     */
    static <K, V> void onResponse(KafkaConsumerRecord<K, V> record, Span span) {
        setCommonTags(span);
        span.setTag("partition", record.partition());
        span.setTag("topic", record.topic());
        span.setTag("offset", record.offset());

    }

    static void onError(Exception exception, Span span) {
        Tags.ERROR.set(span, Boolean.TRUE);
        span.log(errorLogs(exception));
    }

    private static Map<String, Object> errorLogs(Throwable throwable) {
        Map<String, Object> errorLogs = new HashMap<>(4);
        errorLogs.put("event", Tags.ERROR.getKey());
        errorLogs.put("error.kind", throwable.getClass().getName());
        errorLogs.put("error.object", throwable);

        errorLogs.put("message", throwable.getMessage());

        StringWriter sw = new StringWriter();
        throwable.printStackTrace(new PrintWriter(sw));
        errorLogs.put("stack", sw.toString());

        return errorLogs;
    }

    private static void setCommonTags(Span span) {
        Tags.COMPONENT.set(span, COMPONENT_NAME);
        Tags.PEER_SERVICE.set(span, KAFKA_SERVICE);
    }
}
