package org.example.maruko.io;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;
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
            // Build the internal Avro reader with configuration to avoid predicate pushdown issues
            org.apache.hadoop.conf.Configuration enhancedConf = new org.apache.hadoop.conf.Configuration(conf);
            // Disable predicate pushdown that might use statistics incorrectly
            enhancedConf.setBoolean("parquet.avro.add-list-element-records", false);
            // Make sure we read all data, avoid any filtering based on potentially missing statistics
            enhancedConf.setBoolean("parquet.enable.dictionary", false);
            
            // Build the internal Avro reader
            ParquetReader<Object> objectReader = AvroParquetReader
                    .builder(path)
                    // Disable range filtering which might be using null statistics incorrectly
                    .disableCompatibility()
                    .withConf(enhancedConf)
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
            
            for (Schema.Field field : genericRecord.getSchema().getFields()) {
                String fieldName = field.name();
                
                // For union type handling in Avro, we need to check if the value is wrapped in a union
                Object value = genericRecord.get(fieldName);
                
                // If value is null and this is a union type, try to access it differently
                if (value == null && field.schema().getType() == Schema.Type.UNION) {
                    // In some cases, union values might be accessible through specific index access
                    try {
                        // Try to determine the actual non-null type in the union
                        value = getUnionValue(genericRecord, field);
                    } catch (Exception e) {
                        // If union processing fails, use the original value (which might be null)
                        value = genericRecord.get(fieldName);
                    }
                }
                
                // Handle ByteBuffer values that come from Parquet files (common for STRING/BINARY fields in Avro)
                if (value instanceof java.nio.ByteBuffer) {
                    java.nio.ByteBuffer buffer = (java.nio.ByteBuffer) value;
                    byte[] bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                    value = new String(bytes);
                } else if (value instanceof org.apache.avro.util.Utf8) {
                    // Convert Avro Utf8 to String
                    value = value.toString();
                }
                
                // Put the value in the result map
                result.put(fieldName, value);
            }
            
            return result;
        }
        
        return null; // If not a GenericRecord, return null
    }
    
    /**
     * Helper method to handle Avro Union type values properly
     */
    private Object getUnionValue(GenericRecord record, Schema.Field field) {
        try {
            String fieldName = field.name();
            Schema fieldSchema = field.schema();
            
            if (fieldSchema.getType() == Schema.Type.UNION) {
                // Get the value using Avro's specific methods
                Object rawValue = record.get(fieldName);
                
                // If raw value is null, check if it's because union value is not accessed correctly
                if (rawValue == null) {
                    // Try getting by field position instead
                    try {
                        rawValue = record.get(field.pos());
                    } catch (Exception e) {
                        // If position access fails, continue with null value
                    }
                }
                
                if (rawValue != null) {
                    // If value is an Avro Union, get the actual value
                    if (rawValue instanceof org.apache.avro.generic.GenericData.Record) {
                        // If it's a nested record, return as-is
                        return rawValue;
                    } else if (rawValue instanceof org.apache.avro.util.Utf8) {
                        // Convert Utf8 to String
                        return rawValue.toString();
                    } else {
                        // Return the raw value directly
                        return rawValue;
                    }
                }
            }
            
            // If not a union or we couldn't get it differently, return the standard access
            return record.get(fieldName);
            
        } catch (Exception e) {
            // On any error, return the standard access as fallback
            return record.get(field.name());
        }
    }

    @Override
    public void close() throws IOException {
        if (objectReader != null) {
            objectReader.close();
        }
    }
}