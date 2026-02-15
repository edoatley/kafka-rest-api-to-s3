package com.example.kafkarestapi.config;

import com.example.kafkarestapi.avro.Event;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
@ConditionalOnExpression("'${app.kafka.schema-registry-url:}'.trim().length() > 0")
public class SchemaRegistryKafkaConfig {

    @Bean
    public ProducerFactory<String, Event> schemaRegistryProducerFactory(
        @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
        @Value("${app.kafka.schema-registry-url}") String schemaRegistryUrl
    ) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        config.put("schema.registry.url", schemaRegistryUrl);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 268435456);
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean("schemaRegistryKafkaTemplate")
    public KafkaTemplate<String, Event> schemaRegistryKafkaTemplate(
        ProducerFactory<String, Event> schemaRegistryProducerFactory
    ) {
        return new KafkaTemplate<>(schemaRegistryProducerFactory);
    }
}
