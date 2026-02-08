package com.example.kafka.s3.sink.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.example.kafka.s3.sink.avro.DecodedAvro;
import com.example.kafka.s3.sink.testutil.AvroTestData;

class ParquetConverterTest {

	@TempDir
	Path tempDir;

	@Test
	void writesParquetFileWithExpectedRecord() throws Exception {
		Schema schema = AvroTestData.sampleSchema();
		GenericRecord record = AvroTestData.sampleRecord(schema, 7, "payload");
		DecodedAvro decoded = new DecodedAvro(schema, List.of(record));
		Path outputPath = tempDir.resolve("sample.parquet");

		ParquetConverter converter = new ParquetConverter(new Configuration());
		converter.write(decoded, outputPath);

		assertThat(Files.exists(outputPath)).isTrue();

		org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(outputPath.toUri());
		try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(hadoopPath).build()) {
			GenericRecord loaded = reader.read();
			assertThat(loaded.get("id")).isEqualTo(7);
			assertThat(loaded.get("payload").toString()).isEqualTo("payload");
		}
	}
}
