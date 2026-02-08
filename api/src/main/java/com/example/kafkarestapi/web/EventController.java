package com.example.kafkarestapi.web;

import com.example.kafkarestapi.avro.Event;
import com.example.kafkarestapi.service.KafkaEventProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.ResponseEntity;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/events")
public class EventController {

    private final KafkaEventProducer producer;
    private final ObjectMapper objectMapper;

    public EventController(KafkaEventProducer producer, ObjectMapper objectMapper) {
        this.producer = producer;
        this.objectMapper = objectMapper;
    }

    @PostMapping
    public CompletableFuture<ResponseEntity<EventResponse>> ingest(
        @RequestBody EventRequest request,
        @RequestHeader(name = "x-ack-mode", required = false) String ackModeHeader
    ) {
        var timestampMs = request.timestampMs() != null ? request.timestampMs() : System.currentTimeMillis();
        var event = new Event(request.id(), request.type(), timestampMs, request.payload());
        var ackMode = AckMode.fromHeader(ackModeHeader);

        return switch (ackMode) {
            case FIRE_AND_FORGET -> {
                // For 200k TPS, this avoids round-trip latency on the hot path.
                producer.publishFireAndForget(event);
                yield CompletableFuture.completedFuture(
                    ResponseEntity.accepted().body(new EventResponse(event.getId(), "queued"))
                );
            }
            case WAIT_FOR_ACK -> producer.publishWaitForAck(event)
                .thenApply(result -> ResponseEntity.ok(new EventResponse(event.getId(), "acked")));
        };
    }

    @PostMapping(
        value = "/stream",
        consumes = "application/x-ndjson",
        produces = "application/x-ndjson"
    )
    public ResponseBodyEmitter ingestStream(
        HttpServletRequest request,
        @RequestHeader(name = "x-ack-mode", required = false) String ackModeHeader,
        @RequestHeader(name = "x-max-in-flight", required = false) String maxInFlightHeader
    ) {
        var emitter = new ResponseBodyEmitter();
        var ackMode = AckMode.fromHeader(ackModeHeader);
        var maxInFlight = parseMaxInFlight(maxInFlightHeader);
        var backpressure = ackMode == AckMode.WAIT_FOR_ACK && maxInFlight > 0
            ? new Semaphore(maxInFlight)
            : null;
        var inFlight = new AtomicInteger(0);
        var inputDone = new AtomicBoolean(false);

        Thread.ofVirtual().start(() -> {
            try (var reader = new BufferedReader(
                new InputStreamReader(request.getInputStream(), StandardCharsets.UTF_8)
            )) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isBlank()) {
                        continue;
                    }
                    var req = objectMapper.readValue(line, EventRequest.class);
                    var timestampMs = req.timestampMs() != null ? req.timestampMs() : System.currentTimeMillis();
                    var event = new Event(req.id(), req.type(), timestampMs, req.payload());

                    switch (ackMode) {
                        case FIRE_AND_FORGET -> {
                            producer.publishFireAndForget(event);
                            sendAck(emitter, new EventResponse(event.getId(), "queued"));
                        }
                        case WAIT_FOR_ACK -> {
                            if (backpressure != null) {
                                backpressure.acquireUninterruptibly();
                            }
                            inFlight.incrementAndGet();
                            producer.publishWaitForAck(event).whenComplete((result, ex) -> {
                                var status = ex == null ? "acked" : "failed";
                                var response = new EventResponse(event.getId(), status);
                                sendAck(emitter, response);
                                if (backpressure != null) {
                                    backpressure.release();
                                }
                                if (inFlight.decrementAndGet() == 0 && inputDone.get()) {
                                    emitter.complete();
                                }
                            });
                        }
                    }
                }
                inputDone.set(true);
                if (inFlight.get() == 0) {
                    emitter.complete();
                }
            } catch (Exception ex) {
                emitter.completeWithError(ex);
            }
        });

        return emitter;
    }

    private void sendAck(ResponseBodyEmitter emitter, EventResponse response) {
        try {
            var payload = objectMapper.writeValueAsString(response) + "\n";
            emitter.send(payload, MediaType.APPLICATION_NDJSON);
        } catch (Exception ex) {
            emitter.completeWithError(ex);
        }
    }

    private int parseMaxInFlight(String header) {
        if (header == null || header.isBlank()) {
            return 0;
        }
        try {
            return Math.max(Integer.parseInt(header.trim()), 0);
        } catch (NumberFormatException ex) {
            return 0;
        }
    }

    public record EventRequest(String id, String type, String payload, Long timestampMs) {
    }

    public record EventResponse(String id, String status) {
    }

    public enum AckMode {
        FIRE_AND_FORGET,
        WAIT_FOR_ACK;

        public static AckMode fromHeader(String value) {
            return switch (value == null ? "" : value.trim().toLowerCase()) {
                case "", "fire-and-forget", "faf" -> FIRE_AND_FORGET;
                case "wait", "wait-for-ack", "ack" -> WAIT_FOR_ACK;
                default -> FIRE_AND_FORGET;
            };
        }
    }
}
