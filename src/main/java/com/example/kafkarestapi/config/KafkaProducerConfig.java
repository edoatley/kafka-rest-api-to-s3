package com.example.kafkarestapi.config;

import com.example.kafkarestapi.avro.Event;
import java.util.HashMap;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
@EnableConfigurationProperties(AppKafkaProperties.class)
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, Event> producerFactory(KafkaProperties properties) {
        var configs = new HashMap<>(properties.buildProducerProperties());
        return new DefaultKafkaProducerFactory<>(configs);
    }

    @Bean
    public KafkaTemplate<String, Event> kafkaTemplate(ProducerFactory<String, Event> producerFactory) {
        var template = new KafkaTemplate<>(producerFactory);
        template.setObservationEnabled(true);
        return template;
    }
}
