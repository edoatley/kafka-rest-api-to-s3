package com.example.kafkarestapi.service;

import com.example.kafkarestapi.avro.Event;
import com.example.kafkarestapi.config.AppKafkaProperties;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnExpression("'${app.kafka.schema-registry-url:}'.trim().length() > 0")
public class KafkaEventProducerSchemaRegistry {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventProducerSchemaRegistry.class);

    private final KafkaTemplate<String, Event> kafkaTemplate;
    private final String eventsTopic;
    private final boolean localProfileActive;

    public KafkaEventProducerSchemaRegistry(
        @Qualifier("schemaRegistryKafkaTemplate") KafkaTemplate<String, Event> kafkaTemplate,
        AppKafkaProperties properties,
        Environment environment
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.eventsTopic = properties.topic().eventsSchemaRegistry();
        this.localProfileActive = Arrays.asList(environment.getActiveProfiles()).contains("local");
    }

    public void publishFireAndForget(Event event) {
        var future = kafkaTemplate.send(eventsTopic, event.getId(), event);
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                if (localProfileActive) {
                    log.error("Kafka publish failed for id={} topic={}", event.getId(), eventsTopic, ex);
                } else {
                    log.warn("Kafka publish failed for id={} topic={}", event.getId(), eventsTopic, ex);
                }
            } else if (localProfileActive) {
                var metadata = result.getRecordMetadata();
                log.info(
                    "Kafka publish succeeded for id={} topic={} partition={} offset={}",
                    event.getId(),
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset()
                );
            }
        });
    }

    public CompletableFuture<SendResult<String, Event>> publishWaitForAck(Event event) {
        var future = kafkaTemplate.send(eventsTopic, event.getId(), event);
        future.whenComplete((result, ex) -> {
            if (!localProfileActive) {
                return;
            }
            if (ex != null) {
                log.error("Kafka publish failed for id={}", event.getId(), ex);
            } else {
                var metadata = result.getRecordMetadata();
                log.info(
                    "Kafka publish succeeded for id={} topic={} partition={} offset={}",
                    event.getId(),
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset()
                );
            }
        });
        return future;
    }
}
