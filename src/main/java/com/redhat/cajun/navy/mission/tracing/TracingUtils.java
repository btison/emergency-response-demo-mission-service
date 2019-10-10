package com.redhat.cajun.navy.mission.tracing;

import java.util.List;

import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

public class TracingUtils {

    private static final String COMPONENT = "mission-service";

    public static <K, V> void buildAndFinishChildSpan(KafkaConsumerRecord<K, V> record, Tracer tracer) {
        io.opentracing.contrib.kafka.TracingKafkaUtils.buildAndFinishChildSpan(record.record(), tracer);
    }

    public static <K, V> Span buildChildSpan(String operationName, KafkaConsumerRecord<K, V> record, Tracer tracer) {
        SpanContext parentContext = io.opentracing.contrib.kafka.TracingKafkaUtils.extractSpanContext(record.record().headers(), tracer);

        Tracer.SpanBuilder spanBuilder = spanBuilder(operationName, tracer);
        if (parentContext != null) {
            spanBuilder.addReference(References.CHILD_OF, parentContext);
        }
        return spanBuilder.start();
    }

    public static void injectInEventBusMessage(SpanContext spanContext, DeliveryOptions options, Tracer tracer) {
        tracer.inject(spanContext, Format.Builtin.TEXT_MAP, new DeliveryOptionsInjectAdapter(options));
    }

    public static Span buildChildSpan(String operation, Message<JsonObject> message, Tracer tracer) {
        SpanContext parentContext = extractSpanContext(message, tracer);
        Tracer.SpanBuilder spanBuilder = tracer.buildSpan(operation)
                .withTag(Tags.SPAN_KIND, "eventbus.consumer");
        if (parentContext != null) {
            spanBuilder.addReference(References.FOLLOWS_FROM, parentContext);
        }
        return spanBuilder.start();
    }

    public static <K, V> Span buildAndInjectSpan(KafkaProducerRecord<K, V> record, Tracer tracer) {

        String producerOper =
                io.opentracing.contrib.kafka.TracingKafkaUtils.TO_PREFIX + record.topic(); // <======== It provides better readability in the UI
        Tracer.SpanBuilder spanBuilder = tracer
                .buildSpan(producerOper)
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_PRODUCER);

        SpanContext spanContext = io.opentracing.contrib.kafka.TracingKafkaUtils.extractSpanContext(record.record().headers(), tracer);

        if (spanContext != null) {
            spanBuilder.asChildOf(spanContext);
        }

        Span span = spanBuilder.start();
        SpanDecorator.onSend(record, span);

        try {
            TracingUtils.inject(span.context(), record.headers(), tracer);
        } catch (Exception ignore) {
            // it can happen if headers are read only (when record is sent second time)
        }

        return span;
    }

    private static SpanContext extractSpanContext(Message<JsonObject> msg, Tracer tracer) {
        return tracer.extract(Format.Builtin.TEXT_MAP, new MultiMapExtractAdapter(msg.headers()));
    }

    private static Tracer.SpanBuilder spanBuilder(String operationName, Tracer tracer) {
        return tracer.buildSpan(operationName).ignoreActiveSpan()
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CONSUMER)
                .withTag(Tags.COMPONENT.getKey(), COMPONENT);
    }

    static void inject(SpanContext spanContext, List<KafkaHeader> headers, Tracer tracer) {
        tracer.inject(spanContext, Format.Builtin.TEXT_MAP, new KafkaHeadersInjectAdapter(headers));
    }

}
