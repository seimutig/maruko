package org.example.tablestore.flink;

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

import java.io.IOException;
import java.util.*;

/**
 * Output format for writing data to TableStore with proper Flink RowData integration.
 * Also implements SinkFunction to be used directly in DataStream API.
 */
public class TableStoreOutputFormat implements OutputFormat<RowData>, ResultTypeQueryable<RowData>, SinkFunction<RowData>, CheckpointedFunction {
    
    private String tablePath;
    private List<String> primaryKeyFields;
    private List<String> partitionFields;
    private int numBuckets;
    private RowType rowType; // The schema information for RowData
    private org.example.tablestore.core.TableStore tableStore; // The actual TableStore instance
    
    // Temporarily store records before writing
    private Map<String, List<Map<String, Object>>> recordsByPartitionBucket;
    
    public TableStoreOutputFormat(String tablePath, List<String> primaryKeyFields, 
                                 List<String> partitionFields, int numBuckets) {
        this.tablePath = tablePath;
        this.primaryKeyFields = primaryKeyFields != null ? primaryKeyFields : new ArrayList<>();
        this.partitionFields = partitionFields != null ? partitionFields : new ArrayList<>();
        this.numBuckets = numBuckets;
        this.recordsByPartitionBucket = new HashMap<>();
    }

    /**
     * Set the row type to know how to convert RowData records to Map
     */
    public void setRowType(RowType rowType) {
        this.rowType = rowType;
    }

    @Override
    public void configure(Configuration parameters) {
        // Initialize actual TableStore
        try {
            // Create a schema based on the available rowType information
            org.apache.parquet.schema.MessageType schema = createParquetSchemaFromRowType();
            
            // Initialize the actual TableStore
            this.tableStore = new org.example.tablestore.core.TableStore(
                tablePath, 
                primaryKeyFields, 
                partitionFields, 
                numBuckets, 
                schema
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to configure TableStoreOutputFormat", e);
        }
    }
    
    /**
     * Create a Parquet schema from the Flink RowType
     */
    private org.apache.parquet.schema.MessageType createParquetSchemaFromRowType() {
        if (rowType == null) {
            // If rowType is not set, create a default schema
            return new org.apache.parquet.schema.MessageType("tablestore_schema",
                new org.apache.parquet.schema.PrimitiveType(org.apache.parquet.schema.Type.Repetition.REQUIRED, 
                    org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64, "id"));
        }
        
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
                
            org.apache.parquet.schema.Type parquetType = convertLogicalTypeToParquetType(fieldName, logicalType, repetition);
            if (parquetType != null) {
                parquetFields.add(parquetType);
            }
        }
        
        // If no fields were converted, create a minimal schema
        if (parquetFields.isEmpty()) {
            return new org.apache.parquet.schema.MessageType("tablestore_schema",
                new org.apache.parquet.schema.PrimitiveType(org.apache.parquet.schema.Type.Repetition.REQUIRED, 
                    org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64, "id"));
        }
        
        org.apache.parquet.schema.Type[] fieldsArray = parquetFields.toArray(new org.apache.parquet.schema.Type[0]);
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
        // Initialize per-task resources if needed
    }

    @Override
    public void writeRecord(RowData record) throws IOException {
        // Ensure tableStore is initialized - call configure if not yet done
        if (tableStore == null) {
            configure(new org.apache.flink.configuration.Configuration());
        }
        
        // Convert RowData to Map using dynamic schema
        org.example.tablestore.flink.serializer.RowDataToMapConverter converter = 
            new org.example.tablestore.flink.serializer.RowDataToMapConverter(rowType);
        Map<String, Object> recordMap = converter.convert(record);
        
        if (recordMap == null) {
            return;
        }
        
        // Determine partition and bucket (simplified for demo)
        String partitionKey = "default"; // In real implementation, extract from partition fields
        int bucket = 0; // In real implementation, hash bucket assignment
        String partitionBucketKey = partitionKey + "_bucket_" + bucket;
        
        // Accumulate records for buffered writing until checkpoint
        recordsByPartitionBucket.computeIfAbsent(partitionBucketKey, k -> new ArrayList<>())
                                 .add(recordMap);
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
            
            // Actually write to TableStore instead of just printing
            try {
                System.out.println("Writing " + records.size() + " records to TableStore: " + records.subList(0, Math.min(3, records.size())));
                
                // Write records to actual TableStore
                tableStore.write(records);
                
                // Clear the flushed records
                records.clear();
            } catch (Exception e) {
                System.err.println("Error writing records to TableStore: " + e.getMessage());
                throw new IOException("Failed to write records to TableStore", e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        // Flush all accumulated records to files before closing
        System.out.println("Closing TableStoreOutputFormat, flushing " + recordsByPartitionBucket.size() + " partition-bucket groups");
        
        // Process all remaining records
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
        
        if (tableStore != null) {
            // Create a checkpoint in TableStore which will create a snapshot
            tableStore.checkpoint();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // We don't need to restore state in this implementation
        // In a real implementation, we would restore any previously checkpointed state
        System.out.println("Initializing state for TableStoreOutputFormat");
    }
}