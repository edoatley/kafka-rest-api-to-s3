package com.example.kafkarestapi.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.kafka")
public record AppKafkaProperties(Topic topic) {

    public record Topic(String events) {
    }
}
