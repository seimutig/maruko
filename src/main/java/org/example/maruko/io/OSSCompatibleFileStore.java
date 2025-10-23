package org.example.maruko.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;

public class OSSCompatibleFileStore implements IFileStore {
    private String tablePath;
    private Configuration conf;
    private FileSystem fs;
    private MessageType schema;
    private List<String> primaryKeyFields;

    public OSSCompatibleFileStore(String tablePath, MessageType schema) throws IOException {
        this(tablePath, schema, null, null); // Call the new constructor with null options and null primary keys
    }
    
    public OSSCompatibleFileStore(String tablePath, MessageType schema, Map<String, String> options) throws IOException {
        this(tablePath, schema, options, null); // Call the new constructor with null primary keys
    }
    
    public OSSCompatibleFileStore(String tablePath, MessageType schema, Map<String, String> options, List<String> primaryKeyFields) throws IOException {
        // Normalize the table path for OSS compatibility
        this.tablePath = normalizePath(tablePath);
        this.conf = new Configuration();
        
        // Configure for both local and OSS file systems
        setupOSSConfiguration(options);
        
        // Determine the appropriate file system based on the path
        URI uri = URI.create(this.tablePath);
        System.out.println("Attempting to get FileSystem for URI: " + uri);
        
        try {
            this.fs = FileSystem.get(uri, conf);
            System.out.println("Successfully created FileSystem: " + fs.getClass().getName());
            System.out.println("FileSystem URI: " + fs.getUri());
            
            // Verify that we can access the root path
            Path rootPath = new Path(this.tablePath);
            System.out.println("Attempting to check access to root path: " + rootPath);
            
            // Try to create a simple test to verify OSS connection works
            try {
                // Check if we can list the root directory (just for testing connection)
                if (fs.exists(rootPath)) {
                    System.out.println("Root path exists: " + rootPath);
                } else {
                    System.out.println("Root path does not exist: " + rootPath);
                    // Try creating it
                    if (fs.mkdirs(rootPath)) {
                        System.out.println("Successfully created root path: " + rootPath);
                    } else {
                        System.out.println("Failed to create root path: " + rootPath);
                    }
                }
            } catch (Exception e) {
                System.out.println("Could not check or create root path: " + e.getMessage());
            }
            
        } catch (Exception e) {
            System.err.println("Error creating FileSystem: " + e.getMessage());
            e.printStackTrace();
            System.err.println("Available filesystem implementations in configuration:");
            System.err.println("  fs.oss.impl: " + conf.get("fs.oss.impl"));
            System.err.println("  fs.AbstractFileSystem.oss.impl: " + conf.get("fs.AbstractFileSystem.oss.impl"));
            throw new IOException("Failed to create FileSystem for path: " + this.tablePath, e);
        }
        
        this.schema = schema;
        this.primaryKeyFields = primaryKeyFields != null ? primaryKeyFields : Collections.emptyList();
    }
    
    /**
     * Normalizes table path for OSS compatibility
     */
    private String normalizePath(String tablePath) {
        if (tablePath == null) {
            return null;
        }
        
        System.out.println("Original table path: " + tablePath);
        
        // Keep the original format as oss://bucket/path - this is the standard format
        // The endpoint is configured separately via fs.oss.endpoint
        if (tablePath.startsWith("oss://")) {
            return tablePath;
        }
        
        return tablePath;
    }
    
    private void setupOSSConfiguration(Map<String, String> options) {
        // OSS specific configurations - correct class names for Hadoop-aliyun
        conf.set("fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem");
        conf.set("fs.AbstractFileSystem.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem");
        
        // Get configuration from options first (highest priority)
        String endpoint = null;
        String accessKeyId = null;
        String accessKeySecret = null;
        
        if (options != null) {
            endpoint = options.get("oss.endpoint");
            accessKeyId = options.get("oss.accessKeyId");
            accessKeySecret = options.get("oss.accessKeySecret");
        }
        
        // Fall back to system properties if not in options
        if (endpoint == null || endpoint.isEmpty()) {
            endpoint = System.getProperty("oss.endpoint", 
                System.getenv("OSS_ENDPOINT") != null ? System.getenv("OSS_ENDPOINT") : "oss-cn-hangzhou.aliyuncs.com");
        }
        if (accessKeyId == null || accessKeyId.isEmpty()) {
            accessKeyId = System.getProperty("oss.accessKeyId", 
                System.getenv("OSS_ACCESS_KEY_ID") != null ? System.getenv("OSS_ACCESS_KEY_ID") : "");
        }
        if (accessKeySecret == null || accessKeySecret.isEmpty()) {
            accessKeySecret = System.getProperty("oss.accessKeySecret", 
                System.getenv("OSS_ACCESS_KEY_SECRET") != null ? System.getenv("OSS_ACCESS_KEY_SECRET") : "");
        }
        
        if (!accessKeyId.isEmpty() && !accessKeySecret.isEmpty()) {
            conf.set("fs.oss.endpoint", endpoint);
            conf.set("fs.oss.accessKeyId", accessKeyId);
            conf.set("fs.oss.accessKeySecret", accessKeySecret);
        }
        
        System.out.println("OSS Configuration:");
        System.out.println("  Endpoint: " + conf.get("fs.oss.endpoint"));
        System.out.println("  AccessKeyId: " + (conf.get("fs.oss.accessKeyId") != null ? "***" : "not set"));
        System.out.println("  AccessKeySecret: " + (conf.get("fs.oss.accessKeySecret") != null ? "***" : "not set"));
        System.out.println("  OSS Implementation: " + conf.get("fs.oss.impl"));
    }

    public String writeData(List<Map<String, Object>> records, Map<String, String> partitionSpec, int bucket) 
            throws IOException {
        System.out.println("Starting writeData for tablePath: " + tablePath + ", bucket: " + bucket);
        System.out.println("Records to write: " + records.size());
        
        // Create partition and bucket path
        String partitionPath = buildPartitionPath(partitionSpec);
        String bucketPath = String.format("%s/bucket_%05d", partitionPath, bucket);
        String fullBucketPath = tablePath + "/" + bucketPath;
        Path bucketDir = new Path(fullBucketPath);
        
        System.out.println("Creating bucket directory: " + bucketDir.toString());
        
        // Create directory if it doesn't exist
        try {
            if (!fs.exists(bucketDir)) {
                System.out.println("Directory doesn't exist, creating: " + bucketDir);
                boolean created = fs.mkdirs(bucketDir);
                System.out.println("Directory creation result: " + created);
            } else {
                System.out.println("Directory already exists: " + bucketDir);
            }
        } catch (Exception e) {
            System.err.println("Error creating directory " + bucketDir + ": " + e.getMessage());
            e.printStackTrace();
            throw new IOException("Failed to create directory: " + bucketDir, e);
        }

        // Sort records by primary key and sequence number before writing
        List<Map<String, Object>> sortedRecords = new ArrayList<>(records);
        sortedRecords.sort((record1, record2) -> {
            // Extract primary key from configured primary key fields
            String key1 = buildPrimaryKey(record1);
            String key2 = buildPrimaryKey(record2);
            
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
        
        System.out.println("Generated file path: " + filePath);

        // Write actual Parquet file using Avro Parquet writer with statistics enabled
        org.apache.avro.Schema avroSchema = new org.apache.parquet.avro.AvroSchemaConverter().convert(schema);
        
        // Create configuration that enables statistics
        org.apache.hadoop.conf.Configuration statsConf = new org.apache.hadoop.conf.Configuration(conf);
        statsConf.setBoolean("parquet.statistics.enabled", true);
        statsConf.setBoolean("parquet.column.statistics.enabled", true);
        // Enable page-level statistics for better performance
        statsConf.setBoolean("parquet.page.page-verification-enabled", false); // Disable verification for performance
        
        System.out.println("About to write Parquet file: " + filePath);
        
        try {
            // Create HadoopOutputFile with debug info
            org.apache.parquet.hadoop.util.HadoopOutputFile outputFile = 
                org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(filePath, statsConf);
            System.out.println("Created HadoopOutputFile: " + outputFile.getPath());
            
            org.apache.parquet.hadoop.ParquetWriter<org.apache.avro.generic.GenericRecord> writer = 
                (org.apache.parquet.hadoop.ParquetWriter<org.apache.avro.generic.GenericRecord>)
                (org.apache.parquet.hadoop.ParquetWriter<?>)
                org.apache.parquet.avro.AvroParquetWriter
                    .builder(outputFile)
                    .withSchema(avroSchema)
                    .withConf(statsConf)
                    .build();
            
            System.out.println("Created ParquetWriter successfully");
            
            // Write each record to Parquet file by converting Map to GenericRecord
            for (int i = 0; i < sortedRecords.size(); i++) {
                Map<String, Object> recordMap = sortedRecords.get(i);
                org.apache.avro.generic.GenericRecord avroRecord = 
                    createAvroRecordFromMap(recordMap, avroSchema);
                
                System.out.println("About to write record " + i + ": " + recordMap);
                writer.write(avroRecord);
                
                // Print first few records for debugging
                if (i < 3) {
                    System.out.println("Wrote record " + i + ": " + recordMap);
                }
            }
            
            // Explicitly close the writer to ensure data is flushed
            writer.close();
            System.out.println("Closed ParquetWriter successfully");
            
        } catch (Exception e) {
            System.err.println("Error writing Parquet file to " + filePath + ": " + e.getMessage());
            e.printStackTrace();
            throw new IOException("Failed to write Parquet file: " + filePath, e);
        }
        
        // Verify that the file actually exists
        if (fs.exists(filePath)) {
            System.out.println("Successfully wrote and confirmed Parquet file exists: " + filePath);
            System.out.println("File size: " + fs.getFileStatus(filePath).getLen() + " bytes");
        } else {
            System.err.println("ERROR: File does not exist after writing: " + filePath);
            throw new IOException("File was not created at path: " + filePath);
        }
        
        System.out.println("Successfully wrote Parquet file: " + filePath);
        
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
     * Dynamically handles internal fields like _deleted, _sequence_number, etc.
     * even if they're not in the original schema
     */
    private org.apache.avro.generic.GenericRecord createAvroRecordFromMap(
            Map<String, Object> recordMap, org.apache.avro.Schema avroSchema) {
        
        // First, create the base record with the provided schema
        org.apache.avro.generic.GenericRecord avroRecord = 
            new org.apache.avro.generic.GenericData.Record(avroSchema);
        
        // Handle all fields in the original schema
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
        
        // Handle dynamically added internal fields that may not be in the original schema
        // These are typically _deleted, _sequence_number, _deleted_timestamp, etc.
        List<String> internalFieldsAdded = new ArrayList<>();
        for (Map.Entry<String, Object> entry : recordMap.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            
            // Check if this field is not in the original schema
            if (avroSchema.getField(fieldName) == null) {
                // Track internal fields that should be preserved
                if (fieldName.startsWith("_")) {
                    internalFieldsAdded.add(fieldName + "=" + value);
                }
            }
        }
        
        if (!internalFieldsAdded.isEmpty()) {
            System.out.println("Warning: Internal fields not in schema will be lost: " + internalFieldsAdded);
            System.out.println("Recommendation: Include internal fields in Parquet schema:");
            System.out.println("  - _deleted (BOOLEAN, optional)");
            System.out.println("  - _sequence_number (LONG, optional)");  
            System.out.println("  - _deleted_timestamp (LONG, optional)");
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
    
    /**
     * Builds a primary key from the record using configured primary key fields
     */
    private String buildPrimaryKey(Map<String, Object> record) {
        if (record == null || primaryKeyFields.isEmpty()) {
            return ""; // Return empty string if no primary key fields
        }
        
        StringBuilder keyBuilder = new StringBuilder();
        for (String primaryKeyField : primaryKeyFields) {
            Object value = record.get(primaryKeyField);
            if (value != null) {
                if (keyBuilder.length() > 0) {
                    keyBuilder.append("|"); // Separator between multiple primary key fields
                }
                keyBuilder.append(value.toString());
            } else {
                // If any primary key field is null, return empty string to prevent comparison issues
                return "";
            }
        }
        return keyBuilder.length() > 0 ? keyBuilder.toString() : "";
    }
}