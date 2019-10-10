package com.redhat.cajun.navy.mission.message;

import java.util.HashMap;
import java.util.Map;

import com.redhat.cajun.navy.mission.MessageAction;
import com.redhat.cajun.navy.mission.tracing.TracingKafkaConsumer;
import com.redhat.cajun.navy.mission.tracing.TracingUtils;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.consumer.KafkaConsumer;

public class MissionConsumerVerticle extends AbstractVerticle {

    private static final String CACHE_QUEUE = "cache.queue";

    private final Logger logger = LoggerFactory.getLogger(MissionConsumerVerticle.class.getName());

    private Map<String, String> config = new HashMap<>();
    private KafkaConsumer<String, String> consumer = null;

    private Tracer tracer;

    @Override
    public void start(Future<Void> startFuture) throws Exception {

        tracer = GlobalTracer.get();

        String createMissionCommandTopic = config().getString("kafka.sub");

        consumer = TracingKafkaConsumer.create(vertx, ConsumerConfig.getConfig(config()), tracer);

        consumer.handler(record -> {
            Span span = TracingUtils.buildChildSpan("createMissionCommand", record, tracer);
            try {
                DeliveryOptions options = new DeliveryOptions().addHeader("action", MessageAction.CREATE_ENTRY.toString());
                TracingUtils.injectInEventBusMessage(span.context(), options, tracer);
                vertx.eventBus().send(CACHE_QUEUE, record.value(), options, reply -> {
                    if (reply.succeeded()) {
                        logger.debug("Message accepted");
                    } else {
                        logger.error("Incoming Message not accepted " + record.topic());
                        logger.error(record.value());
                        Tags.ERROR.set(span, Boolean.TRUE);
                    }
                });
            } finally {
                span.finish();
            }
        });

        consumer.subscribe(createMissionCommandTopic, ar -> {
            if (ar.succeeded()) {
                logger.info("subscribed to MissionCommand");
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
