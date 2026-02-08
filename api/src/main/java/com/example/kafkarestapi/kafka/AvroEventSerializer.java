package com.example.kafkarestapi.kafka;

import com.example.kafkarestapi.avro.Event;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class AvroEventSerializer implements Serializer<Event> {

    @Override
    public byte[] serialize(String topic, Event data) {
        if (data == null) {
            return null;
        }

        var writer = new SpecificDatumWriter<>(Event.getClassSchema());
        try (var output = new ByteArrayOutputStream(128)) {
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(output, null);
            writer.write(data, encoder);
            encoder.flush();
            return output.toByteArray();
        } catch (IOException ex) {
            throw new SerializationException("Failed to serialize Event", ex);
        }
    }
}
