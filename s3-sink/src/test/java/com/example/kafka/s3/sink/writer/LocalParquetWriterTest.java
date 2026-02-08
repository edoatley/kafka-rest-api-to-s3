package com.example.kafka.s3.sink.writer;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.example.kafka.s3.sink.avro.DecodedAvro;
import com.example.kafka.s3.sink.parquet.ParquetConverter;
import com.example.kafka.s3.sink.testutil.AvroTestData;

class LocalParquetWriterTest {

	@TempDir
	Path tempDir;

	@Test
	void writesParquetFileToLocalDirectory() {
		Schema schema = AvroTestData.sampleSchema();
		GenericRecord record = AvroTestData.sampleRecord(schema, 1, "local");
		DecodedAvro decoded = new DecodedAvro(schema, List.of(record));

		ParquetConverter converter = new ParquetConverter(new Configuration());
		LocalParquetWriter writer = new LocalParquetWriter(converter);

		Path outputPath = tempDir.resolve("out").resolve("record.parquet");
		Path written = writer.write(decoded, outputPath);

		assertThat(Files.exists(written)).isTrue();
	}
}
