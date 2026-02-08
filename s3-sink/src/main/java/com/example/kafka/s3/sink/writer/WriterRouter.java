package com.example.kafka.s3.sink.writer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

import com.example.kafka.s3.sink.avro.DecodedAvro;
import com.example.kafka.s3.sink.config.AppProperties;
import com.example.kafka.s3.sink.config.TargetType;
import com.example.kafka.s3.sink.mapping.TopicMappingResolver;
import java.time.Clock;

import jakarta.annotation.PreDestroy;

@Component
public class WriterRouter {

	private final TopicMappingResolver resolver;
	private final LocalParquetWriter localWriter;
	private final S3ParquetWriter s3Writer;
	private final AppProperties.BatchProperties batch;
	private final Clock clock;
	private final Map<String, BatchBuffer> buffers = new ConcurrentHashMap<>();
	private final ScheduledExecutorService scheduler;

	public WriterRouter(TopicMappingResolver resolver,
			LocalParquetWriter localWriter,
			S3ParquetWriter s3Writer,
			AppProperties properties,
			Clock clock) {
		this.resolver = resolver;
		this.localWriter = localWriter;
		this.s3Writer = s3Writer;
		this.batch = properties.getBatch();
		this.clock = clock;
		this.scheduler = createScheduler();
	}

	public void write(String topic, DecodedAvro decoded) {
		if (decoded == null || decoded.records().isEmpty()) {
			return;
		}
		List<DecodedAvro> batches = new ArrayList<>();
		BatchBuffer buffer = buffers.computeIfAbsent(topic, ignored -> new BatchBuffer());
		buffer.lock.lock();
		try {
			if (buffer.schema != null && !buffer.schema.equals(decoded.schema())) {
				DecodedAvro drained = buffer.drain();
				if (drained != null) {
					batches.add(drained);
				}
				buffer.schema = decoded.schema();
			} else if (buffer.schema == null) {
				buffer.schema = decoded.schema();
			}

			buffer.records.addAll(decoded.records());
			buffer.lastAppend = Instant.now(clock);

			if (buffer.records.size() >= batch.getMaxRecords()) {
				DecodedAvro drained = buffer.drain();
				if (drained != null) {
					batches.add(drained);
				}
			}
		} finally {
			buffer.lock.unlock();
		}

		for (DecodedAvro batchToWrite : batches) {
			writeNow(topic, batchToWrite);
		}
	}

	@PreDestroy
	public void shutdown() {
		try {
			flushAll();
		} finally {
			scheduler.shutdownNow();
		}
	}

	private ScheduledExecutorService createScheduler() {
		Duration interval = batch.getFlushInterval();
		if (interval == null || interval.isZero() || interval.isNegative()) {
			return Executors.newSingleThreadScheduledExecutor(runnable -> {
				Thread thread = new Thread(runnable, "parquet-batch-flusher");
				thread.setDaemon(true);
				return thread;
			});
		}

		ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
			Thread thread = new Thread(runnable, "parquet-batch-flusher");
			thread.setDaemon(true);
			return thread;
		});
		executor.scheduleAtFixedRate(this::flushExpired, interval.toMillis(),
				interval.toMillis(), TimeUnit.MILLISECONDS);
		return executor;
	}

	private void flushExpired() {
		Duration interval = batch.getFlushInterval();
		if (interval == null || interval.isZero() || interval.isNegative()) {
			return;
		}
		Instant now = Instant.now(clock);
		for (Map.Entry<String, BatchBuffer> entry : buffers.entrySet()) {
			String topic = entry.getKey();
			BatchBuffer buffer = entry.getValue();
			DecodedAvro drained = null;
			buffer.lock.lock();
			try {
				if (buffer.lastAppend != null
						&& Duration.between(buffer.lastAppend, now).compareTo(interval) >= 0) {
					drained = buffer.drain();
				}
			} finally {
				buffer.lock.unlock();
			}
			if (drained != null) {
				writeNow(topic, drained);
			}
		}
	}

	private void flushAll() {
		for (Map.Entry<String, BatchBuffer> entry : buffers.entrySet()) {
			String topic = entry.getKey();
			BatchBuffer buffer = entry.getValue();
			DecodedAvro drained = null;
			buffer.lock.lock();
			try {
				drained = buffer.drain();
			} finally {
				buffer.lock.unlock();
			}
			if (drained != null) {
				writeNow(topic, drained);
			}
		}
	}

	private void writeNow(String topic, DecodedAvro decoded) {
		WriteTarget target = resolver.resolve(topic);
		if (target.type() == TargetType.LOCAL) {
			localWriter.write(decoded, target.localPath());
			return;
		}
		s3Writer.write(decoded, target.bucket(), target.key());
	}

	private static class BatchBuffer {
		private final ReentrantLock lock = new ReentrantLock();
		private Schema schema;
		private final List<GenericRecord> records = new ArrayList<>();
		private Instant lastAppend;

		private DecodedAvro drain() {
			if (records.isEmpty() || schema == null) {
				return null;
			}
			List<GenericRecord> snapshot = List.copyOf(records);
			records.clear();
			lastAppend = null;
			return new DecodedAvro(schema, snapshot);
		}
	}
}
