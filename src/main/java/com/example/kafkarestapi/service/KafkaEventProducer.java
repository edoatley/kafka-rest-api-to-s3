package com.example.kafkarestapi.service;

import com.example.kafkarestapi.avro.Event;
import com.example.kafkarestapi.config.AppKafkaProperties;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
public class KafkaEventProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventProducer.class);

    private final KafkaTemplate<String, Event> kafkaTemplate;
    private final String eventsTopic;

    public KafkaEventProducer(KafkaTemplate<String, Event> kafkaTemplate, AppKafkaProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.eventsTopic = properties.topic().events();
    }

    public void publishFireAndForget(Event event) {
        var future = kafkaTemplate.send(eventsTopic, event.getId(), event);
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.warn("Kafka publish failed for id={}", event.getId(), ex);
            }
        });
    }

    public CompletableFuture<SendResult<String, Event>> publishWaitForAck(Event event) {
        return kafkaTemplate.send(eventsTopic, event.getId(), event);
    }
}
