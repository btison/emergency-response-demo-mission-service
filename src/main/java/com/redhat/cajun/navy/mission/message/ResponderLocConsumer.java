package com.redhat.cajun.navy.mission.message;

import com.redhat.cajun.navy.mission.MessageAction;
import com.redhat.cajun.navy.mission.tracing.TracingKafkaConsumer;
import com.redhat.cajun.navy.mission.tracing.TracingUtils;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.consumer.KafkaConsumer;

public class ResponderLocConsumer extends AbstractVerticle {

    private final Logger logger = LoggerFactory.getLogger(ResponderLocConsumer.class.getName());
    private KafkaConsumer<String, String> consumer = null;
    private static final String CACHE_QUEUE = "cache.queue";

    private Tracer tracer;

    @Override
    public void start(Future<Void> startFuture) throws Exception {

        tracer = GlobalTracer.get();

        String responderLocationUpdateTopic = config().getString("kafka.sub.responder.loc.update");
        consumer = TracingKafkaConsumer.create(vertx,  ConsumerConfig.getConfig(config()), tracer);

        consumer.handler(record -> {
            Span span = TracingUtils.buildChildSpan("responderLocationUpdate", record, tracer);
            DeliveryOptions options = new DeliveryOptions().addHeader("action", MessageAction.UPDATE_ENTRY.toString());
            TracingUtils.injectInEventBusMessage(span.context(), options, tracer);
            vertx.eventBus().send(CACHE_QUEUE, record.value(), options, reply -> {
                if (reply.failed()) {
                    System.err.println("Incoming Message not accepted "+record.topic());
                    System.err.println(record.value());
                }
            });
            span.finish();

        });


        consumer.subscribe(responderLocationUpdateTopic, ar -> {
            if (ar.succeeded()) {
                logger.info("subscribed to ResponderLocationUpdate");
            } else {
                logger.fatal("Could not subscribe " + ar.cause().getMessage());
            }
        });

    }

    @Override
    public void stop() throws Exception {
        consumer.unsubscribe(ar -> {

            if (ar.succeeded()) {
                logger.info("Consumer unsubscribed");
            }
        });
    }

}
