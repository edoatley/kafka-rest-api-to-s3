package com.example.kafkarestapi.web;

import com.example.kafkarestapi.avro.Event;
import com.example.kafkarestapi.service.KafkaEventProducerSchemaRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;

@RestController
@RequestMapping("/events/v2")
@ConditionalOnExpression("'${app.kafka.schema-registry-url:}'.trim().length() > 0")
public class EventControllerV2 {

    private final KafkaEventProducerSchemaRegistry producer;
    private final ObjectMapper objectMapper;

    public EventControllerV2(KafkaEventProducerSchemaRegistry producer, ObjectMapper objectMapper) {
        this.producer = producer;
        this.objectMapper = objectMapper;
    }

    @PostMapping
    public CompletableFuture<ResponseEntity<EventController.EventResponse>> ingest(
        @RequestBody EventController.EventRequest request,
        @RequestHeader(name = "x-ack-mode", required = false) String ackModeHeader
    ) {
        var timestampMs = request.timestampMs() != null ? request.timestampMs() : System.currentTimeMillis();
        var event = new Event(request.id(), request.type(), timestampMs, request.payload());
        var ackMode = EventController.AckMode.fromHeader(ackModeHeader);

        return switch (ackMode) {
            case FIRE_AND_FORGET -> {
                producer.publishFireAndForget(event);
                yield CompletableFuture.completedFuture(
                    ResponseEntity.accepted().body(new EventController.EventResponse(event.getId(), "queued"))
                );
            }
            case WAIT_FOR_ACK -> producer.publishWaitForAck(event)
                .thenApply(result -> ResponseEntity.ok(new EventController.EventResponse(event.getId(), "acked")));
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
        var ackMode = EventController.AckMode.fromHeader(ackModeHeader);
        var maxInFlight = parseMaxInFlight(maxInFlightHeader);
        var backpressure = ackMode == EventController.AckMode.WAIT_FOR_ACK && maxInFlight > 0
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
                    var req = objectMapper.readValue(line, EventController.EventRequest.class);
                    var timestampMs = req.timestampMs() != null ? req.timestampMs() : System.currentTimeMillis();
                    var event = new Event(req.id(), req.type(), timestampMs, req.payload());

                    switch (ackMode) {
                        case FIRE_AND_FORGET -> {
                            producer.publishFireAndForget(event);
                            sendAck(emitter, new EventController.EventResponse(event.getId(), "queued"));
                        }
                        case WAIT_FOR_ACK -> {
                            if (backpressure != null) {
                                backpressure.acquireUninterruptibly();
                            }
                            inFlight.incrementAndGet();
                            producer.publishWaitForAck(event).whenComplete((result, ex) -> {
                                var status = ex == null ? "acked" : "failed";
                                var response = new EventController.EventResponse(event.getId(), status);
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

    private void sendAck(ResponseBodyEmitter emitter, EventController.EventResponse response) {
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

}
