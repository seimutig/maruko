package org.example.maruko.flink;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.BooleanType;
import org.example.maruko.MarukoLogger;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Output format for writing data to Maruko with proper Flink RowData integration.
 * Also implements SinkFunction to be used directly in DataStream API.
 */
public class MarukoOutputFormat implements OutputFormat<RowData>, ResultTypeQueryable<RowData>, SinkFunction<RowData>, CheckpointedFunction {
    
    private static final Logger logger = MarukoLogger.getLogger(MarukoOutputFormat.class);
    private String tablePath;
    private List<String> primaryKeyFields;
    private List<String> partitionFields;
    private int numBuckets;
    private RowType rowType; // The schema information for RowData
    private org.example.maruko.core.TableStore tableStore; // The actual Maruko instance
    
    // Temporarily store records before writing
    private Map<String, List<Map<String, Object>>> recordsByPartitionBucket;
    
    private Map<String, String> options;

    public MarukoOutputFormat(String tablePath, List<String> primaryKeyFields, 
                                 List<String> partitionFields, int numBuckets) {
        this(tablePath, primaryKeyFields, partitionFields, numBuckets, null);
    }
    
    public MarukoOutputFormat(String tablePath, List<String> primaryKeyFields, 
                                 List<String> partitionFields, int numBuckets, Map<String, String> options) {
        this.tablePath = tablePath;
        this.primaryKeyFields = primaryKeyFields != null ? primaryKeyFields : new ArrayList<>();
        this.partitionFields = partitionFields != null ? partitionFields : new ArrayList<>();
        this.numBuckets = numBuckets;
        this.options = options != null ? options : new HashMap<>();
        this.recordsByPartitionBucket = new HashMap<>();
        
        // Set OSS-related system properties from options if available
        if (this.options != null) {
            String endpoint = this.options.get("oss.endpoint");
            String accessKeyId = this.options.get("oss.accessKeyId");
            String accessKeySecret = this.options.get("oss.accessKeySecret");
            
            if (endpoint != null && !endpoint.isEmpty()) {
                System.setProperty("oss.endpoint", endpoint);
            }
            if (accessKeyId != null && !accessKeyId.isEmpty()) {
                System.setProperty("oss.accessKeyId", accessKeyId);
            }
            if (accessKeySecret != null && !accessKeySecret.isEmpty()) {
                System.setProperty("oss.accessKeySecret", accessKeySecret);
            }
        }
    }

    /**
     * Set the row type to know how to convert RowData records to Map
     */
    public void setRowType(RowType rowType) {
        this.rowType = rowType;
    }

    @Override
    public void configure(Configuration parameters) {
        // Set OSS-related system properties from options before initializing TableStore
        // This is crucial for Flink cluster environment where each task manager needs these properties
        if (this.options != null) {
            String endpoint = this.options.get("oss.endpoint");
            String accessKeyId = this.options.get("oss.accessKeyId");
            String accessKeySecret = this.options.get("oss.accessKeySecret");
            
            if (endpoint != null && !endpoint.isEmpty()) {
                System.setProperty("oss.endpoint", endpoint);
            }
            if (accessKeyId != null && !accessKeyId.isEmpty()) {
                System.setProperty("oss.accessKeyId", accessKeyId);
            }
            if (accessKeySecret != null && !accessKeySecret.isEmpty()) {
                System.setProperty("oss.accessKeySecret", accessKeySecret);
            }
        }
        
        // Initialize actual TableStore
        try {
            // Create a schema based on the available rowType information
            org.apache.parquet.schema.MessageType schema = createParquetSchemaFromRowType();
            
            // Initialize the actual TableStore with options
            this.tableStore = new org.example.maruko.core.TableStore(
                tablePath, 
                primaryKeyFields, 
                partitionFields, 
                numBuckets, 
                schema,
                options
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to configure MarukoOutputFormat", e);
        }
    }
    
    /**
     * Create a Parquet schema from the Flink RowType
     * Includes both business fields and internal TableStore fields
     */
    private org.apache.parquet.schema.MessageType createParquetSchemaFromRowType() {
        if (rowType == null) {
            // If rowType is not set, log a warning and try to continue
            System.out.println("MarukoOutputFormat: WARNING - rowType is null, schema inference needed from actual data");
            // Create schema with just internal fields since we don't have business schema
            return createInternalFieldsOnlySchema();
        }
        
        System.out.println("MarukoOutputFormat: Creating Parquet schema from RowType with " + rowType.getFieldCount() + " fields: " + rowType.getFieldNames());
        
        // Build Parquet fields from Flink RowType
        List<org.apache.parquet.schema.Type> parquetFields = new ArrayList<>();
        List<org.apache.flink.table.types.logical.LogicalType> fieldTypes = rowType.getChildren();
        List<String> fieldNames = rowType.getFieldNames();
        
        for (int i = 0; i < Math.min(fieldTypes.size(), fieldNames.size()); i++) {
            String fieldName = fieldNames.get(i);
            org.apache.flink.table.types.logical.LogicalType logicalType = fieldTypes.get(i);
            org.apache.parquet.schema.Type.Repetition repetition = logicalType.isNullable() ? 
                org.apache.parquet.schema.Type.Repetition.OPTIONAL : 
                org.apache.parquet.schema.Type.Repetition.REQUIRED;
                
            System.out.println("MarukoOutputFormat: Converting field '" + fieldName + "' with type " + logicalType);
            org.apache.parquet.schema.Type parquetType = convertLogicalTypeToParquetType(fieldName, logicalType, repetition);
            if (parquetType != null) {
                System.out.println("MarukoOutputFormat: Successfully converted field '" + fieldName + "' to Parquet type: " + parquetType);
                parquetFields.add(parquetType);
            } else {
                System.out.println("MarukoOutputFormat: Failed to convert field '" + fieldName + "'");
            }
        }
        
        // ALWAYS add internal Maruko fields to ensure proper functioning
        addInternalMarukoFields(parquetFields);
        
        // If no business fields were converted, still include internal fields
        if (parquetFields.isEmpty()) {
            System.out.println("MarukoOutputFormat: WARNING - No business fields were converted from rowType, creating schema with internal fields only");
            return createInternalFieldsOnlySchema();
        }
        
        System.out.println("MarukoOutputFormat: Successfully created Parquet schema with " + parquetFields.size() + " fields (business + internal fields)");
        org.apache.parquet.schema.Type[] fieldsArray = parquetFields.toArray(new org.apache.parquet.schema.Type[0]);
        org.apache.parquet.schema.MessageType schema = new org.apache.parquet.schema.MessageType("maruko_schema", fieldsArray);
        System.out.println("MarukoOutputFormat: Final schema: " + schema);
        return schema;
    }
    
    /**
     * Add internal Maruko fields to the schema
     * These are required for proper Maruko functioning
     */
    private void addInternalMarukoFields(List<org.apache.parquet.schema.Type> parquetFields) {
        // Add internal fields that Maruko needs for proper operation
        parquetFields.add(new org.apache.parquet.schema.PrimitiveType(
            org.apache.parquet.schema.Type.Repetition.OPTIONAL,
            org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN,
            "_deleted"
        ));
        
        parquetFields.add(new org.apache.parquet.schema.PrimitiveType(
            org.apache.parquet.schema.Type.Repetition.OPTIONAL,
            org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64,
            "_sequence_number"
        ));
        
        parquetFields.add(new org.apache.parquet.schema.PrimitiveType(
            org.apache.parquet.schema.Type.Repetition.OPTIONAL,
            org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64,
            "_deleted_timestamp"
        ));
        
        System.out.println("MarukoOutputFormat: Added internal Maruko fields (_deleted, _sequence_number, _deleted_timestamp)");
    }
    
    /**
     * Create a schema with only internal TableStore fields
     * Used when no business schema is available
     */
    private org.apache.parquet.schema.MessageType createInternalFieldsOnlySchema() {
        List<org.apache.parquet.schema.Type> internalFields = new ArrayList<>();
        addInternalMarukoFields(internalFields);
        
        org.apache.parquet.schema.Type[] fieldsArray = internalFields.toArray(new org.apache.parquet.schema.Type[0]);
        return new org.apache.parquet.schema.MessageType("tablestore_schema", fieldsArray);
    }
    
    /**
     * Convert Flink LogicalType to Parquet Type
     */
    private org.apache.parquet.schema.Type convertLogicalTypeToParquetType(String fieldName, 
            org.apache.flink.table.types.logical.LogicalType logicalType, 
            org.apache.parquet.schema.Type.Repetition repetition) {
        
        if (logicalType instanceof org.apache.flink.table.types.logical.BigIntType) {
            return new org.apache.parquet.schema.PrimitiveType(repetition, 
                org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64, fieldName);
        } else if (logicalType instanceof org.apache.flink.table.types.logical.IntType) {
            return new org.apache.parquet.schema.PrimitiveType(repetition, 
                org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32, fieldName);
        } else if (logicalType instanceof org.apache.flink.table.types.logical.DoubleType) {
            return new org.apache.parquet.schema.PrimitiveType(repetition, 
                org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE, fieldName);
        } else if (logicalType instanceof org.apache.flink.table.types.logical.VarCharType) {
            return new org.apache.parquet.schema.PrimitiveType(repetition, 
                org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY, fieldName);
        } else if (logicalType instanceof org.apache.flink.table.types.logical.BooleanType) {
            return new org.apache.parquet.schema.PrimitiveType(repetition, 
                org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN, fieldName);
        } else if (logicalType instanceof org.apache.flink.table.types.logical.TimestampType) {
            return new org.apache.parquet.schema.PrimitiveType(repetition, 
                org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64, fieldName);
        } else {
            // For other types, default to INT64
            return new org.apache.parquet.schema.PrimitiveType(repetition, 
                org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64, fieldName);
        }
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        // Initialize tableStore during open to ensure table structure exists before any checkpoint
        // This prevents issues where checkpoint occurs before first writeRecord call
        if (tableStore == null) {
            System.out.println("Initializing tableStore in open() method for task " + taskNumber + 
                             " at path: " + tablePath);
            try {
                // Create a schema based on the available rowType information
                org.apache.parquet.schema.MessageType schema = createParquetSchemaFromRowType();
                
                // Set OSS-related system properties from options before initializing TableStore
                // This is crucial for Flink cluster environment where each task manager needs these properties
                if (this.options != null) {
                    String endpoint = this.options.get("oss.endpoint");
                    String accessKeyId = this.options.get("oss.accessKeyId");
                    String accessKeySecret = this.options.get("oss.accessKeySecret");
                    
                    if (endpoint != null && !endpoint.isEmpty()) {
                        System.setProperty("oss.endpoint", endpoint);
                    }
                    if (accessKeyId != null && !accessKeyId.isEmpty()) {
                        System.setProperty("oss.accessKeyId", accessKeyId);
                    }
                    if (accessKeySecret != null && !accessKeySecret.isEmpty()) {
                        System.setProperty("oss.accessKeySecret", accessKeySecret);
                    }
                }
                
                // Initialize the actual TableStore with options
                this.tableStore = new org.example.maruko.core.TableStore(
                    tablePath, 
                    primaryKeyFields, 
                    partitionFields, 
                    numBuckets, 
                    schema,
                    options
                );
                
                System.out.println("tableStore initialized successfully in open() method. Table structure created at: " + tablePath);
            } catch (Exception e) {
                System.err.println("Error initializing tableStore in open() method: " + e.getMessage());
                e.printStackTrace();
                throw new IOException("Failed to initialize tableStore", e);
            }
        }
    }

    // Variables for buffer management
    // Optimized for low-throughput scenarios like datagen with 1 record per 2 seconds
    private volatile long lastFlushTime = System.currentTimeMillis();
    private static final long FLUSH_INTERVAL_MS = 30000; // 30 seconds - more appropriate for low throughput
    private static final int MAX_RECORDS_PER_PARTITION_BUCKET = 10; // Much lower threshold for faster flushes
    private volatile int totalRecordsProcessed = 0; // Track total records for monitoring
    
    @Override
    public void writeRecord(RowData record) throws IOException {
        // Ensure tableStore is initialized - call configure if not yet done
        if (tableStore == null) {
            configure(new org.apache.flink.configuration.Configuration());
        }
        
        // Convert RowData to Map using dynamic schema
        if (rowType == null) {
            System.out.println("MarukoOutputFormat: rowType is null, cannot convert RowData to Map");
            return;
        }
        
        org.example.maruko.flink.serializer.RowDataToMapConverter converter = 
            new org.example.maruko.flink.serializer.RowDataToMapConverter(rowType);
        Map<String, Object> recordMap = converter.convert(record);
        
        if (recordMap == null) {
            System.out.println("MarukoOutputFormat: Record conversion returned null, skipping");
            return;
        }
        
        // Debug: Check if the recordMap is empty
        if (recordMap.isEmpty() && rowType.getFields().size() > 0) {
            System.out.println("MarukoOutputFormat: Record conversion returned empty map, but rowType has " + 
                             rowType.getFields().size() + " fields: " + rowType.getFieldNames());
            System.out.println("Raw RowData might have values but conversion failed. RowType: " + rowType);
        }
        
        // Determine partition from partition fields, if any
        Map<String, String> partitionSpec = new HashMap<>();
        if (partitionFields != null) {
            for (String partitionField : partitionFields) {
                Object value = recordMap.get(partitionField);
                if (value != null) {
                    partitionSpec.put(partitionField, value.toString());
                } else {
                    partitionSpec.put(partitionField, "default");
                }
            }
        }
        String partitionKey = buildPartitionPath(partitionSpec);
        
        // Determine bucket based on primary key fields
        int bucket = 0;
        if (primaryKeyFields != null && !primaryKeyFields.isEmpty()) {
            bucket = calculateBucketFromFields(primaryKeyFields, recordMap, numBuckets);
        }
        
        String partitionBucketKey = partitionKey + "_bucket_" + bucket;
        
        // Accumulate records for buffered writing until checkpoint
        List<Map<String, Object>> bucketRecords = recordsByPartitionBucket.computeIfAbsent(partitionBucketKey, k -> new ArrayList<>());
        bucketRecords.add(recordMap);
        totalRecordsProcessed++;
        
        // Debug: Print first few records as they come in
        if (totalRecordsProcessed <= 5) {
            System.out.println("MarukoOutputFormat: Received record #" + totalRecordsProcessed + 
                             ", partitionBucketKey: " + partitionBucketKey + 
                             ", record: " + recordMap);
        }
        
        // Check if we should flush this partition-bucket due to size
        if (bucketRecords.size() >= MAX_RECORDS_PER_PARTITION_BUCKET) {
            logger.debug("MarukoOutputFormat: Flushing bucket due to size threshold reached: {} >= {}", 
                        bucketRecords.size(), MAX_RECORDS_PER_PARTITION_BUCKET);
            flushPartitionBucket(partitionBucketKey);
        }
        
        // Check if time-based flush is needed for all buckets
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastFlushTime >= FLUSH_INTERVAL_MS) {
            logger.debug("MarukoOutputFormat: Time-based flush triggered after {}ms, processed {} records", 
                        (currentTime - lastFlushTime), totalRecordsProcessed);
            // Flush all buckets to prevent memory buildup
            flushAllBuffers();
            lastFlushTime = currentTime;
        }
    }
    
    /**
     * Flush records for a specific partition-bucket combination
     */
    private void flushPartitionBucket(String partitionBucketKey) throws IOException {
        List<Map<String, Object>> records = recordsByPartitionBucket.get(partitionBucketKey);
        if (records != null && !records.isEmpty()) {
            // Ensure tableStore is initialized before trying to write
            if (tableStore == null) {
                configure(new org.apache.flink.configuration.Configuration());
            }
            
            // Actually write to Maruko instead of just printing
            try {
                logger.debug("Writing {} records to Maruko: {}", records.size(), records.subList(0, Math.min(3, records.size())));
                
                // Write records to actual Maruko
                tableStore.write(records);
                
                // Clear the flushed records
                records.clear();
            } catch (Exception e) {
                logger.error("Error writing records to Maruko: {}", e.getMessage());
                throw new IOException("Failed to write records to Maruko", e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        // Flush all accumulated records to files before closing
        logger.debug("Closing MarukoOutputFormat, flushing {} partition-bucket groups", recordsByPartitionBucket.size());
        
        // Process all remaining records
        flushAllBuffers();
    }
    
    /**
     * Flush all accumulated buffers to the TableStore
     */
    private void flushAllBuffers() throws IOException {
        // Process all accumulated records
        for (String partitionBucketKey : recordsByPartitionBucket.keySet()) {
            flushPartitionBucket(partitionBucketKey);
        }
        
        recordsByPartitionBucket.clear();
    }
    
    @Override
    public TypeInformation<RowData> getProducedType() {
        if (rowType != null) {
            return org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(rowType);
        }
        // Default row type if not specified - this is just a placeholder
        return org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(org.apache.flink.table.types.logical.RowType.of());
    }
    
    // SinkFunction implementation
    
    @Override
    public void invoke(RowData value, Context context) throws Exception {
        // Simply delegate to writeRecord
        writeRecord(value);
    }

    // CheckpointedFunction implementation
    
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // This is called when a checkpoint is being performed
        // We should checkpoint the accumulated records and create a snapshot
        System.out.println("Creating snapshot during checkpoint with ID: " + context.getCheckpointId());
        
        // Flush all accumulated records before creating the snapshot
        flushAllBuffers();
        
        // Ensure tableStore is initialized before checkpoint - this is critical because checkpoint
        // can happen before any data records are processed, and the table structure needs to exist
        if (tableStore == null) {
            System.out.println("tableStore not initialized during checkpoint, initializing now...");
            try {
                // Create a schema based on the available rowType information
                org.apache.parquet.schema.MessageType schema = createParquetSchemaFromRowType();
                
                // Initialize the actual TableStore with options
                this.tableStore = new org.example.maruko.core.TableStore(
                    tablePath, 
                    primaryKeyFields, 
                    partitionFields, 
                    numBuckets, 
                    schema,
                    options
                );
                
                System.out.println("tableStore initialized in snapshotState method. Table structure created at: " + tablePath);
            } catch (Exception e) {
                System.err.println("Error initializing tableStore during checkpoint: " + e.getMessage());
                e.printStackTrace();
                throw e;
            }
        } else {
            System.out.println("tableStore already exists, checkpoint will use existing structure at: " + tablePath);
        }
        
        // Actually perform checkpoint to create manifest and snapshot files
        // This is essential for proper table structure with manifest and snapshots
        if (tableStore != null) {
            System.out.println("Calling tableStore.checkpoint() to create manifest and snapshot files...");
            tableStore.checkpoint();
            System.out.println("TableStore checkpoint completed successfully, manifest and snapshot created.");
        } else {
            System.out.println("tableStore is null, skipping checkpoint operation.");
        }
        
        System.out.println("Checkpoint completed for MarukoOutputFormat with ID: " + context.getCheckpointId() + 
                          ". Table structure verified at: " + tablePath);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // We don't need to restore state in this implementation
        // In a real implementation, we would restore any previously checkpointed state
        System.out.println("Initializing state for MarukoOutputFormat");
    }
    
    /**
     * Builds a partition path from a partition specification
     */
    private String buildPartitionPath(Map<String, String> partitionSpec) {
        StringBuilder path = new StringBuilder();
        for (Map.Entry<String, String> entry : partitionSpec.entrySet()) {
            if (path.length() > 0) {
                path.append("/");
            }
            path.append(entry.getKey()).append("=").append(entry.getValue());
        }
        return path.length() > 0 ? path.toString() : "default";
    }
    
    /**
     * Calculates bucket number based on primary key fields using hash
     */
    private int calculateBucketFromFields(List<String> keyFields, Map<String, Object> record, int numBuckets) {
        if (keyFields.isEmpty()) {
            return 0; // Default bucket if no key fields
        }
        
        // Create a string representation of the key fields values
        StringBuilder keyBuilder = new StringBuilder();
        for (String keyField : keyFields) {
            Object value = record.get(keyField);
            if (value != null) {
                if (keyBuilder.length() > 0) {
                    keyBuilder.append("|"); // Separator
                }
                keyBuilder.append(value.toString());
            }
        }
        
        // Calculate hash and map to bucket range
        String key = keyBuilder.toString();
        int hash = key.hashCode();
        return Math.abs(hash) % numBuckets;
    }
}