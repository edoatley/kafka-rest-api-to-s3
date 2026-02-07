package com.example.kafkarestapi;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

@EmbeddedKafka(partitions = 1, topics = "events")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "app.security.enabled=false",
        "spring.kafka.properties.security.protocol=PLAINTEXT",
        "spring.kafka.properties.ssl.keystore.location=",
        "spring.kafka.properties.ssl.keystore.password=",
        "spring.kafka.properties.ssl.truststore.location=",
        "spring.kafka.properties.ssl.truststore.password=",
        "logging.level.org.apache.kafka.storage.internals.log=OFF"
    }
)
@Import(EmbeddedKafkaIntegrationTest.TestSecurityConfig.class)
class EmbeddedKafkaIntegrationTest {

    @LocalServerPort
    private int port;

    @Test
    void shouldAcceptEvent() throws Exception {
        var payload = """
            {"id":"evt-embedded-1","type":"user.created","payload":"{\\"userId\\":\\"123\\"}"}
            """.trim();
        var client = HttpClient.newHttpClient();
        var request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + "/events"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(payload, StandardCharsets.UTF_8))
            .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
    }

    @Test
    void shouldStreamAcksForBatch() throws Exception {
        var requestBody = String.join(
            "\n",
            "{\"id\":\"evt-embedded-10\",\"type\":\"user.created\",\"payload\":\"{\\\"userId\\\":\\\"10\\\"}\"}",
            "{\"id\":\"evt-embedded-11\",\"type\":\"user.updated\",\"payload\":\"{\\\"userId\\\":\\\"11\\\"}\"}",
            ""
        );

        var client = HttpClient.newHttpClient();
        var request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + "/events/stream"))
            .header("Content-Type", "application/x-ndjson")
            .header("x-ack-mode", "wait")
            .header("x-max-in-flight", "1")
            .POST(HttpRequest.BodyPublishers.ofString(requestBody, StandardCharsets.UTF_8))
            .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());

        assertThat(response.statusCode()).isEqualTo(200);
        try (var reader = new BufferedReader(
            new InputStreamReader(response.body(), StandardCharsets.UTF_8)
        )) {
            var lines = new ArrayList<String>(2);
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank()) {
                    lines.add(line);
                }
                if (lines.size() == 2) {
                    break;
                }
            }
            assertThat(lines).hasSize(2);
            assertThat(lines.get(0)).contains("\"id\":\"evt-embedded-10\"");
            assertThat(lines.get(1)).contains("\"id\":\"evt-embedded-11\"");
        }
    }

    @TestConfiguration
    static class TestSecurityConfig {
        @Bean
        SecurityFilterChain testSecurityFilterChain(HttpSecurity http) throws Exception {
            return http
                .csrf(csrf -> csrf.disable())
                .authorizeHttpRequests(auth -> auth.anyRequest().permitAll())
                .build();
        }
    }
}
