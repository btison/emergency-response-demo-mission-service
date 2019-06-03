package com.redhat.cajun.navy.mission.message;

import java.util.HashMap;
import java.util.Map;

import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;

public class ConsumerConfig {


    public static Map<String, String> getConfig(JsonObject vertxConfig){

        Map<String, String> config = new HashMap<>();
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, vertxConfig.getString("kafka.connect", "localhost:9092"));
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, vertxConfig.getString("kafka.group.id"));
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, vertxConfig.getBoolean("kafka.autocommit", true).toString());
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, vertxConfig.getString("kafka.security.protocol"));
        config.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, vertxConfig.getString("kafka.ssl.keystore.type"));
        config.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, vertxConfig.getString("kafka.ssl.keystore.location"));
        config.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, vertxConfig.getString("kafka.ssl.keystore.password"));
        config.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, vertxConfig.getString("kafka.ssl.truststore.type"));
        config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, vertxConfig.getString("kafka.ssl.truststore.location"));
        config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, vertxConfig.getString("kafka.ssl.truststore.password"));

        return config;
    }

}
