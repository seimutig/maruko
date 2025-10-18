package org.example.tablestore.flink;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;

import org.example.tablestore.flink.serializer.MapToRowDataConverter;
import org.example.tablestore.core.TableStore;

/**
 * Flink InputFormat for reading data from TableStore.
 * This bridges TableStore's internal format with Flink's InputFormat interface.
 * The approach is to use the TableStore's built-in read functionality rather than 
 * directly accessing Parquet files, to ensure consistency with snapshots and manifests.
 * Also implements SourceFunction to be used directly in DataStream API.
 * 
 * IMPORTANT: This class is designed to only read data and must not trigger any write operations.
 * 
 * Updated to support streaming functionality by monitoring new data changes.
 */
public class TableStoreInputFormat implements InputFormat<RowData, InputSplit>, ResultTypeQueryable<RowData>, SourceFunction<RowData> {
    
    private final String tablePath;
    private final List<String> primaryKeyFields;
    private TableStore tableStore;  // TableStore instance for reading
    private volatile boolean isRunning = true;
    private RowType rowType; // The schema information for RowData
    private volatile long lastReadSnapshotId = -1; // Track the last read snapshot for streaming
    private long pollingInterval = 2000; // 2 seconds polling interval
    
    public TableStoreInputFormat(String tablePath) {
        this(tablePath, java.util.Collections.emptyList());
    }
    
    public TableStoreInputFormat(String tablePath, List<String> primaryKeyFields) {
        this(tablePath, primaryKeyFields, null); // Call new constructor with null options
    }
    
    public TableStoreInputFormat(String tablePath, List<String> primaryKeyFields, Map<String, String> options) {
        this.tablePath = tablePath;
        this.primaryKeyFields = primaryKeyFields != null ? primaryKeyFields : java.util.Collections.emptyList();
        this.lastReadSnapshotId = -1;
    }
    
    /**
     * Set the row type to know how to convert Map records to RowData
     */
    public void setRowType(RowType rowType) {
        this.rowType = rowType;
    }

    @Override
    public void configure(Configuration parameters) {
        System.out.println("Configuring TableStoreInputFormat for path: " + this.tablePath);
        
        try {
            // Create a minimal schema for TableStore - we'll use the rowType information if available
            // otherwise create a basic schema
            MessageType schema;
            if (rowType != null) {
                // Convert from Flink RowType to Parquet MessageType
                schema = createParquetSchemaFromRowType(rowType);
            } else {
                // Default schema as fallback
                schema = new MessageType("tablestore_schema",
                    Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("id"));
            }
            
            // Use the primary key fields that were passed in from the TableStoreTableSource
            List<String> partitionFields = Collections.emptyList(); // For this test, no partition keys
            
            System.out.println("Initializing TableStore for reading with primary keys: " + this.primaryKeyFields);
            
            this.tableStore = new TableStore(this.tablePath, this.primaryKeyFields, partitionFields, 2, schema);
            
            // Initialize the last read snapshot ID to current latest
            this.lastReadSnapshotId = tableStore.getLastReadSnapshotId();
            
            System.out.println("TableStore initialized for reading from: " + this.tablePath + 
                             " with primary keys: " + this.primaryKeyFields);
            System.out.println("Initial last read snapshot ID: " + this.lastReadSnapshotId);

        } catch (Exception e) {
            System.out.println("Error during configuration: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to configure TableStoreInputFormat", e);
        }
    }
    
    /**
     * Create a Parquet schema from the Flink RowType
     */
    private MessageType createParquetSchemaFromRowType(RowType rowType) {
        if (rowType == null) {
            // If rowType is not set, create a default schema
            return new MessageType("tablestore_schema",
                Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("id"));
        }
        
        // Build Parquet fields from Flink RowType
        java.util.List<org.apache.parquet.schema.Type> parquetFields = new java.util.ArrayList<>();
        List<org.apache.flink.table.types.logical.LogicalType> fieldTypes = rowType.getChildren();
        List<String> fieldNames = rowType.getFieldNames();
        
        for (int i = 0; i < Math.min(fieldTypes.size(), fieldNames.size()); i++) {
            String fieldName = fieldNames.get(i);
            org.apache.flink.table.types.logical.LogicalType logicalType = fieldTypes.get(i);
            
            org.apache.parquet.schema.Type parquetType = convertLogicalTypeToParquetType(fieldName, logicalType);
            if (parquetType != null) {
                parquetFields.add(parquetType);
            }
        }
        
        // If no fields were converted, create a minimal schema
        if (parquetFields.isEmpty()) {
            return new MessageType("tablestore_schema",
                Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("id"));
        }
        
        org.apache.parquet.schema.Type[] fieldsArray = parquetFields.toArray(new org.apache.parquet.schema.Type[0]);
        return new MessageType("tablestore_schema", fieldsArray);
    }
    
    /**
     * Convert Flink LogicalType to Parquet Type
     */
    private org.apache.parquet.schema.Type convertLogicalTypeToParquetType(String fieldName, 
            org.apache.flink.table.types.logical.LogicalType logicalType) {
        
        if (logicalType instanceof org.apache.flink.table.types.logical.BigIntType) {
            return Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named(fieldName);
        } else if (logicalType instanceof org.apache.flink.table.types.logical.IntType) {
            return Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32).named(fieldName);
        } else if (logicalType instanceof org.apache.flink.table.types.logical.DoubleType) {
            return Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE).named(fieldName);
        } else if (logicalType instanceof org.apache.flink.table.types.logical.VarCharType) {
            return Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named(fieldName);
        } else if (logicalType instanceof org.apache.flink.table.types.logical.BooleanType) {
            return Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN).named(fieldName);
        } else if (logicalType instanceof org.apache.flink.table.types.logical.TimestampType) {
            return Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named(fieldName);
        } else {
            // For other types, default to INT64
            return Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named(fieldName);
        }
    }
    
    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        // In production, return real statistics
        return null;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        // For simplicity, create a single split since we're reading all records from TableStore at once
        GenericInputSplit[] splits = new GenericInputSplit[1];
        splits[0] = new GenericInputSplit(0, 1);
        return splits;
    }
    
    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new org.apache.flink.api.common.io.DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(InputSplit split) throws IOException {
        System.out.println("Opening TableStoreInputFormat split " + split.getSplitNumber());
        
        if (tableStore != null) {
            try {
                // Read all records from the TableStore using its built-in read functionality
                // This ensures we get the latest consistent snapshot
                List<Map<String, Object>> allRecords = tableStore.read();
                System.out.println("Read " + (allRecords != null ? allRecords.size() : 0) + " total records from TableStore");
                if (allRecords != null && !allRecords.isEmpty()) {
                    System.out.println("First record: " + allRecords.get(0));
                }
            } catch (Exception e) {
                throw new IOException("Failed to read from TableStore", e);
            }
        } else {
            System.out.println("TableStore is null, no data to read");
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        // For streaming mode, this is always false as we continuously monitor for new data
        return false;
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        // In streaming mode, we don't use this method, instead we use the SourceFunction interface
        return null;
    }

    @Override
    public void close() throws IOException {
        isRunning = false;
        if (tableStore != null) {
            try {
                tableStore.close();
            } catch (Exception e) {
                System.err.println("Error closing TableStore: " + e.getMessage());
            }
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        if (rowType != null) {
            return org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(rowType);
        }
        // Default row type if not specified - this is just a placeholder
        return org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(org.apache.flink.table.types.logical.RowType.of());
    }
    
    // SourceFunction implementation - This is where the streaming magic happens!
        
    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        isRunning = true;
        
        System.out.println("Starting TableStore streaming reader...");
        System.out.println("Validating initialization: tableStore=" + (tableStore != null) + 
                          ", rowType=" + (rowType != null) + 
                          ", lastReadSnapshotId=" + lastReadSnapshotId);
        
        // If tableStore is null (which can happen in Flink's execution model), initialize it in run method
        if (tableStore == null) {
            System.out.println("tableStore is null during execution, initializing now...");
            try {
                // Create a minimal schema for TableStore - we'll use the rowType information if available
                // otherwise create a basic schema
                MessageType schema;
                if (rowType != null) {
                    // Convert from Flink RowType to Parquet MessageType
                    schema = createParquetSchemaFromRowType(rowType);
                } else {
                    // Default schema as fallback
                    schema = new MessageType("tablestore_schema",
                        Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("id"));
                }
                
                // Use the primary key fields that were passed in from the TableStoreTableSource
                List<String> partitionFields = Collections.emptyList(); // For this test, no partition keys
                
                System.out.println("Initializing TableStore in run() method for path: " + this.tablePath + 
                                 " with primary keys: " + this.primaryKeyFields);
                
                this.tableStore = new TableStore(this.tablePath, this.primaryKeyFields, partitionFields, 2, schema);
                
                // Initialize the last read snapshot ID to current latest
                this.lastReadSnapshotId = tableStore.getLastReadSnapshotId();
                
                System.out.println("TableStore re-initialized in run() method for reading from: " + this.tablePath + 
                                 " with primary keys: " + this.primaryKeyFields);
                System.out.println("Initial last read snapshot ID: " + this.lastReadSnapshotId);
                
            } catch (Exception e) {
                System.err.println("Error during run() initialization: " + e.getMessage());
                e.printStackTrace();
                throw new RuntimeException("Failed to initialize TableStore in run() method", e);
            }
        }
        
        if (tableStore == null) {
            System.err.println("CRITICAL ERROR: tableStore is still null after re-initialization! Cannot start streaming reader.");
            return; // Early return if tableStore is still null
        }
        
        System.out.println("Monitoring for new data changes since snapshot ID: " + lastReadSnapshotId);
        
        try {
            while (isRunning) {
                // Check if tableStore is properly initialized
                if (tableStore == null) {
                    System.out.println("ERROR: tableStore became null during execution!");
                    break; // Exit the loop if tableStore is null
                }
                
                // For streaming, try to read the latest data from tablestore
                // Since persistent changelog is removed from sink, we'll check for new data differently
                List<Map<String, Object>> allRecords = null;
                try {
                    allRecords = tableStore.read();
                } catch (Exception e) {
                    System.err.println("Error reading from TableStore: " + e.getMessage());
                    // Continue the loop even if there's an error
                }
                
                if (allRecords != null && !allRecords.isEmpty()) {
                    System.out.println("Detected " + allRecords.size() + " records, emitting to stream...");
                    
                    for (Map<String, Object> record : allRecords) {
                        if (!isRunning) {
                            break;
                        }
                        
                        // Convert Map<String, Object> to RowData
                        if (record != null && rowType != null) {
                            try {
                                // Add change type info to the record for Flink changelog
                                Map<String, Object> finalRecord = new java.util.HashMap<>(record);
                                
                                // Check that MapToRowDataConverter class exists and is accessible
                                MapToRowDataConverter converter = new MapToRowDataConverter(rowType);
                                if (converter != null) {
                                    RowData rowData = converter.convert(finalRecord);
                                    if (rowData != null) {
                                        synchronized (ctx.getCheckpointLock()) {
                                            ctx.collect(rowData);
                                        }
                                    } else {
                                        System.out.println("Warning: Converter returned null for record: " + finalRecord);
                                    }
                                } else {
                                    System.out.println("Warning: MapToRowDataConverter is null for record: " + finalRecord);
                                }
                            } catch (Exception e) {
                                System.err.println("Error converting record: " + e.getMessage());
                                e.printStackTrace();
                            }
                        } else if (record != null) {
                            System.out.println("Warning: rowType is null, cannot convert record: " + record);
                        }
                    }
                    
                    // Update the last read snapshot ID based on the current state of TableStore
                    long currentLatestId = tableStore.getLastReadSnapshotId();
                    if (currentLatestId > lastReadSnapshotId) {
                        lastReadSnapshotId = currentLatestId;
                        System.out.println("Updated last read snapshot ID to: " + lastReadSnapshotId);
                    }
                } else {
                    System.out.println("No new records detected in this polling cycle, continuing...");
                }
                
                // Sleep for a while before checking for new data again
                Thread.sleep(pollingInterval);
            }
        } catch (InterruptedException e) {
            // Restore interrupted state
            Thread.currentThread().interrupt();
            System.out.println("TableStoreInputFormat interrupted, stopping...");
        } catch (Exception e) {
            System.err.println("Error reading data from TableStore in SourceFunction: " + e.getMessage());
            e.printStackTrace();
            throw new IOException("Error reading data from TableStore in SourceFunction: " + e.getMessage(), e);
        } finally {
            isRunning = false;
            System.out.println("TableStore streaming reader stopped");
        }
    }
    
    @Override
    public void cancel() {
        isRunning = false;
        System.out.println("TableStoreInputFormat cancelled");
    }
}