package com.example.kafka.s3.sink.parquet;

import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.avro.AvroParquetWriter;
import org.springframework.stereotype.Component;

import com.example.kafka.s3.sink.avro.DecodedAvro;

@Component
public class ParquetConverter {

	private final Configuration hadoopConfiguration;

	public ParquetConverter(Configuration hadoopConfiguration) {
		this.hadoopConfiguration = hadoopConfiguration;
	}

	public void write(DecodedAvro decoded, java.nio.file.Path outputPath) throws IOException {
		org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(outputPath.toUri());
		try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
				.withSchema(decoded.schema())
				.withConf(hadoopConfiguration)
				.withCompressionCodec(CompressionCodecName.SNAPPY)
				.build()) {
			for (GenericRecord record : decoded.records()) {
				writer.write(record);
			}
		}
	}
}
