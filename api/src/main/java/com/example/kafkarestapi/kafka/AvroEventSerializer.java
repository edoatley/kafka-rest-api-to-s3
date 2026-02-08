package com.example.kafkarestapi.kafka;

import com.example.kafkarestapi.avro.Event;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.file.DataFileWriter;
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
        try (var output = new ByteArrayOutputStream(256);
                var dataFileWriter = new DataFileWriter<>(writer)) {
            dataFileWriter.create(Event.getClassSchema(), output);
            dataFileWriter.append(data);
            dataFileWriter.flush();
            return output.toByteArray();
        } catch (IOException ex) {
            throw new SerializationException("Failed to serialize Event", ex);
        }
    }
}
