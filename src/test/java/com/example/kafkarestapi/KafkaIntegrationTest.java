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
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class KafkaIntegrationTest {

    @LocalServerPort
    private int port;

    private static final Network network = Network.newNetwork();
    @Container
    @SuppressWarnings("resource")
    private static final KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.1")
    ).withNetwork(network).withNetworkAliases("kafka");

    private final TestRestTemplate restTemplate;

    KafkaIntegrationTest(TestRestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @DynamicPropertySource
    static void kafkaProps(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Test
    void shouldAcceptEvent() {
        var payload = Map.of(
            "id", "evt-1",
            "type", "user.created",
            "payload", "{\"userId\":\"123\"}"
        );

        ResponseEntity<String> response = restTemplate.postForEntity("/events", payload, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
    }

    @Test
    void shouldStreamAcksForBatch() throws Exception {
        var requestBody = String.join(
            "\n",
            "{\"id\":\"evt-10\",\"type\":\"user.created\",\"payload\":\"{\\\"userId\\\":\\\"10\\\"}\"}",
            "{\"id\":\"evt-11\",\"type\":\"user.updated\",\"payload\":\"{\\\"userId\\\":\\\"11\\\"}\"}",
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
            assertThat(lines.get(0)).contains("\"id\":\"evt-10\"");
            assertThat(lines.get(1)).contains("\"id\":\"evt-11\"");
        }
    }

    @TestConfiguration
    static class TestSecurityConfig {
        @Bean
        SecurityFilterChain testSecurityFilterChain(HttpSecurity http) throws Exception {
            return http.authorizeHttpRequests(auth -> auth.anyRequest().permitAll()).build();
        }
    }
}
