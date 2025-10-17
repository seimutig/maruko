package org.example.tablestore.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.apache.parquet.io.api.RecordConsumer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

public class FileStore {
    private String tablePath;
    private Configuration conf;
    private FileSystem fs;
    private MessageType schema;

    public FileStore(String tablePath, MessageType schema) throws IOException {
        this.tablePath = tablePath;
        this.conf = new Configuration();
        this.fs = FileSystem.get(conf);
        this.schema = schema;
    }

    public String writeData(List<Map<String, Object>> records, Map<String, String> partitionSpec, int bucket) 
            throws IOException {
        // Create partition and bucket path
        String partitionPath = buildPartitionPath(partitionSpec);
        String bucketPath = String.format("%s/bucket_%05d", partitionPath, bucket);
        Path bucketDir = new Path(tablePath + "/" + bucketPath);
        
        // Create directory if it doesn't exist
        if (!fs.exists(bucketDir)) {
            fs.mkdirs(bucketDir);
        }

        // Sort records by primary key (assuming 'id' is primary key) and sequence number before writing
        // In a real implementation, we'd extract the primary key fields from the table configuration
        List<Map<String, Object>> sortedRecords = new ArrayList<>(records);
        sortedRecords.sort((record1, record2) -> {
            // Extract primary key - assuming 'id' is the primary key field
            String key1 = record1.get("id") != null ? record1.get("id").toString() : "";
            String key2 = record2.get("id") != null ? record2.get("id").toString() : "";
            
            // First compare by primary key
            int primaryKeyCompare = key1.compareTo(key2);
            if (primaryKeyCompare != 0) {
                return primaryKeyCompare;
            }
            
            // If primary keys are equal, compare by sequence number (higher sequence is more recent)
            Object seq1 = record1.get("_sequence_number");
            Object seq2 = record2.get("_sequence_number");
            
            if (seq1 == null && seq2 == null) {
                return 0;
            } else if (seq1 == null) {
                return -1;
            } else if (seq2 == null) {
                return 1;
            } else {
                // Assuming sequence numbers are longs or Long objects
                Long s1 = seq1 instanceof Long ? (Long) seq1 : Long.valueOf(seq1.toString());
                Long s2 = seq2 instanceof Long ? (Long) seq2 : Long.valueOf(seq2.toString());
                return s2.compareTo(s1); // Reverse order to get newer records first during merge
            }
        });

        // Generate a unique filename for new data file
        long timestamp = System.currentTimeMillis();
        String fileName = String.format("data_%05d.parquet", timestamp);
        Path filePath = new Path(bucketDir, fileName);

        // Write actual Parquet file using Avro Parquet writer
        org.apache.avro.Schema avroSchema = new org.apache.parquet.avro.AvroSchemaConverter().convert(schema);
        try (org.apache.parquet.hadoop.ParquetWriter<org.apache.avro.generic.GenericRecord> writer = 
                (org.apache.parquet.hadoop.ParquetWriter<org.apache.avro.generic.GenericRecord>)
                (org.apache.parquet.hadoop.ParquetWriter<?>)
                org.apache.parquet.avro.AvroParquetWriter
                    .builder(org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(filePath, conf))
                    .withSchema(avroSchema)
                    .withConf(conf)
                    .build()) {
            
            // Write each record to Parquet file by converting Map to GenericRecord
            for (Map<String, Object> recordMap : sortedRecords) {
                org.apache.avro.generic.GenericRecord avroRecord = 
                    createAvroRecordFromMap(recordMap, avroSchema);
                writer.write(avroRecord);
            }
        }
        
        // Return the actual file path
        return filePath.toString();
    }

    private String buildPartitionPath(Map<String, String> partitionSpec) {
        StringBuilder path = new StringBuilder();
        for (Map.Entry<String, String> entry : partitionSpec.entrySet()) {
            if (path.length() > 0) {
                path.append("/");
            }
            path.append(entry.getKey()).append("=").append(entry.getValue());
        }
        return path.toString();
    }

    public String getTablePath() {
        return tablePath;
    }

    public FileSystem getFileSystem() {
        return fs;
    }
    
    /**
     * Creates an Avro GenericRecord from a Map<String, Object> based on the schema
     */
    private org.apache.avro.generic.GenericRecord createAvroRecordFromMap(
            Map<String, Object> recordMap, org.apache.avro.Schema avroSchema) {
        
        org.apache.avro.generic.GenericRecord avroRecord = 
            new org.apache.avro.generic.GenericData.Record(avroSchema);
        
        for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
            String fieldName = field.name();
            Object value = recordMap.get(fieldName);
            
            // Handle null values and type conversions
            if (value != null) {
                org.apache.avro.Schema fieldSchema = field.schema();
                avroRecord.put(fieldName, convertValueToAvroType(value, fieldSchema));
            } else {
                avroRecord.put(fieldName, null);
            }
        }
        
        return avroRecord;
    }
    
    /**
     * Converts a value to the appropriate Avro type based on the field schema
     */
    private Object convertValueToAvroType(Object value, org.apache.avro.Schema fieldSchema) {
        if (value == null) {
            return null;
        }
        
        org.apache.avro.Schema.Type fieldType = fieldSchema.getType();
        
        // Handle union types (which include null)
        if (fieldType == org.apache.avro.Schema.Type.UNION) {
            for (org.apache.avro.Schema unionType : fieldSchema.getTypes()) {
                if (unionType.getType() != org.apache.avro.Schema.Type.NULL) {
                    return convertValueToAvroType(value, unionType);
                }
            }
        }
        
        // Handle different primitive types
        switch (fieldType) {
            case STRING:
                return new org.apache.avro.util.Utf8(value.toString());
            case INT:
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                } else {
                    try {
                        return Integer.parseInt(value.toString());
                    } catch (NumberFormatException e) {
                        return 0;
                    }
                }
            case LONG:
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                } else {
                    try {
                        return Long.parseLong(value.toString());
                    } catch (NumberFormatException e) {
                        return 0L;
                    }
                }
            case FLOAT:
                if (value instanceof Number) {
                    return ((Number) value).floatValue();
                } else {
                    try {
                        return Float.parseFloat(value.toString());
                    } catch (NumberFormatException e) {
                        return 0.0f;
                    }
                }
            case DOUBLE:
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                } else {
                    try {
                        return Double.parseDouble(value.toString());
                    } catch (NumberFormatException e) {
                        return 0.0;
                    }
                }
            case BOOLEAN:
                if (value instanceof Boolean) {
                    return (Boolean) value;
                } else {
                    return Boolean.parseBoolean(value.toString());
                }
            case BYTES:
                if (value instanceof byte[]) {
                    return java.nio.ByteBuffer.wrap((byte[]) value);
                } else {
                    return java.nio.ByteBuffer.wrap(value.toString().getBytes());
                }
            case ENUM:
                return new org.apache.avro.util.Utf8(value.toString());
            default:
                // For default, handle as string
                return new org.apache.avro.util.Utf8(value.toString());
        }
    }
}