package com.redhat.cajun.navy.mission;


import com.redhat.cajun.navy.mission.http.MissionRestVerticle;
import com.redhat.cajun.navy.mission.message.MissionConsumerVerticle;
import com.redhat.cajun.navy.mission.message.MissionProducerVerticle;
import com.redhat.cajun.navy.mission.message.ResponderLocConsumer;
import io.jaegertracing.Configuration;
import io.opentracing.util.GlobalTracer;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;


public class MissionMain extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(MissionMain.class.getName());

    private static ConfigRetrieverOptions selectConfigOptions(){
        ConfigRetrieverOptions options = new ConfigRetrieverOptions();

        if (System.getenv("KUBERNETES_NAMESPACE") != null) {
            ConfigStoreOptions appStore = new ConfigStoreOptions()
                    .setType("file")
                    .setFormat("properties")
                    .setConfig(new JsonObject()
                            .put("name", System.getenv("APP_CONFIGMAP_NAME"))
                            .put("key", System.getenv("APP_CONFIGMAP_KEY"))
                            .put("path", "/deployments/config/app-config.properties"));
            options.addStore(appStore);
        } else {
            ConfigStoreOptions props = new ConfigStoreOptions()
                    .setType("file")
                    .setFormat("properties")
                    .setConfig(new JsonObject().put("path", "local-app-config.properties"));
            options.addStore(props);
        }

        return options;
    }


    private static void deployVerticles(Vertx vertx, JsonObject config, Future<Void> future){

        Future<String> tracerFuture = Future.future();
        initTracer(config, tracerFuture.completer());

        Future<String> rFuture = Future.future();
        Future<String> cFuture = Future.future();
        Future<String> pFuture = Future.future();
        Future<String> lFuture = Future.future();

        DeploymentOptions options = new DeploymentOptions();

        options.setConfig(config);
        vertx.deployVerticle(new MissionRestVerticle(), options, rFuture.completer());
        vertx.deployVerticle(new MissionConsumerVerticle(), options, cFuture.completer());
        vertx.deployVerticle(new MissionProducerVerticle(), options, cFuture.completer());
        vertx.deployVerticle(new ResponderLocConsumer(), options, lFuture.completer());

        CompositeFuture.all(tracerFuture, rFuture, cFuture, pFuture, lFuture).setHandler(ar -> {
            if (ar.succeeded()) {
                logger.info("Verticles deployed successfully.");
                future.complete();
            } else {
                logger.error("WARNINIG: Verticles NOT deployed successfully.");
                future.fail(ar.cause());
            }
        });
    }

    private static void initTracer(JsonObject config, Handler<AsyncResult<String>> completionHandler) {
        String serviceName = config.getString("tracing.service-name");
        if (serviceName == null || serviceName.isEmpty()) {
            logger.warn("No Service Name set. Skipping initialization of the Jaeger Tracer.");
            reportResult(completionHandler, Future.succeededFuture());
            return;
        }

        Configuration configuration = new Configuration(serviceName)
                .withSampler(new Configuration.SamplerConfiguration()
                        .withType(config.getString("tracing.sampler-type"))
                        .withParam(getPropertyAsNumber(config, "tracing.sampler-param"))
                        .withManagerHostPort(config.getString("tracing.sampler-manager-host-port")))
                .withReporter(new Configuration.ReporterConfiguration()
                        .withLogSpans(config.getBoolean("tracing.reporter-log-spans"))
                        .withFlushInterval(config.getInteger("tracing.reporter-flush-interval"))
                        .withMaxQueueSize(config.getInteger("tracing.max-queue-size"))
                        .withSender(new Configuration.SenderConfiguration()
                                .withAgentHost("localhost")
                                .withAgentPort(6831)));
        GlobalTracer.registerIfAbsent(configuration.getTracer());
        reportResult(completionHandler, Future.succeededFuture());
    }

    private static void reportResult(Handler<AsyncResult<String>> completionHandler, AsyncResult<String> result) {
        if (completionHandler != null) {
            completionHandler.handle(result);
        }
    }

    private static Number getPropertyAsNumber(JsonObject json, String key) {
        Object o  = json.getValue(key);
        if (o instanceof Number) {
            return (Number) o;
        }
        return null;
    }

    // Entry point for the app
    public static void main(String[] args) {
        io.vertx.core.Vertx vertx = io.vertx.core.Vertx.vertx(new VertxOptions().setMetricsOptions(
                new MicrometerMetricsOptions()
                        .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                        .setEnabled(true)));

        Future<Void> future = Future.future();
        ConfigRetriever.create(vertx, selectConfigOptions())
                .getConfig(ar -> {
                    if (ar.succeeded()) {
                        deployVerticles(vertx, ar.result(), future);
                    } else {
                        logger.fatal("Failed to retrieve the configuration.");
                        future.fail(ar.cause());
                    }
                });
    }

}
