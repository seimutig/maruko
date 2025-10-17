package org.example.tablestore.io;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SimpleMapParquetReader implements AutoCloseable {
    private final ParquetReader<Object> objectReader;

    public SimpleMapParquetReader(ParquetReader<Object> objectReader) {
        this.objectReader = objectReader;
    }

    public static class Builder {
        private final Path path;

        public Builder(Path path) {
            this.path = path;
        }

        public SimpleMapParquetReader build(Configuration conf) throws IOException {
            // Build the internal Avro reader
            ParquetReader<Object> objectReader = AvroParquetReader
                    .builder(path)
                    .withConf(conf)
                    .build();
            return new SimpleMapParquetReader(objectReader);
        }
    }

    public Map<String, Object> read() throws IOException {
        Object record = objectReader.read();
        if (record == null) {
            return null;
        }

        // Convert GenericRecord to Map<String, Object>
        if (record instanceof GenericRecord) {
            GenericRecord genericRecord = (GenericRecord) record;
            Map<String, Object> result = new HashMap<>();
            for (org.apache.avro.Schema.Field field : genericRecord.getSchema().getFields()) {
                Object value = genericRecord.get(field.name());
                // Convert ByteBuffer to String for BINARY fields to match expected string values
                if (value instanceof java.nio.ByteBuffer) {
                    java.nio.ByteBuffer buffer = (java.nio.ByteBuffer) value;
                    byte[] bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                    result.put(field.name(), new String(bytes));
                } else {
                    result.put(field.name(), value);
                }
            }
            return result;
        }
        
        return null; // If not a GenericRecord, return null
    }

    @Override
    public void close() throws IOException {
        if (objectReader != null) {
            objectReader.close();
        }
    }
}