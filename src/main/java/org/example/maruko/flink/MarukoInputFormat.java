package org.example.maruko.flink;

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

import org.example.maruko.flink.serializer.MapToRowDataConverter;
import org.example.maruko.core.TableStore;

/**
 * Flink InputFormat for reading data from Maruko.
 * This bridges Maruko's internal format with Flink's InputFormat interface.
 * The approach is to use the TableStore's built-in read functionality rather than 
 * directly accessing Parquet files, to ensure consistency with snapshots and manifests.
 * Also implements SourceFunction to be used directly in DataStream API.
 * 
 * IMPORTANT: This class is designed to only read data and must not trigger any write operations.
 * 
 * Updated to support streaming functionality by monitoring new data changes.
 */
public class MarukoInputFormat implements InputFormat<RowData, InputSplit>, ResultTypeQueryable<RowData>, SourceFunction<RowData> {
    
    private final String tablePath;
    private final List<String> primaryKeyFields;
    private TableStore tableStore;  // TableStore instance for reading
    private volatile boolean isRunning = true;
    private RowType rowType; // The schema information for RowData
    private volatile long lastReadSnapshotId = -1; // Track the last read snapshot for streaming
    private long pollingInterval = 2000; // 2 seconds polling interval
    private final Map<String, String> options; // Configuration options including OSS settings
    
    public MarukoInputFormat(String tablePath) {
        this(tablePath, java.util.Collections.emptyList());
    }
    
    public MarukoInputFormat(String tablePath, List<String> primaryKeyFields) {
        this(tablePath, primaryKeyFields, null); // Call new constructor with null options
    }
    
    public MarukoInputFormat(String tablePath, List<String> primaryKeyFields, Map<String, String> options) {
        this.tablePath = tablePath;
        this.primaryKeyFields = primaryKeyFields != null ? primaryKeyFields : java.util.Collections.emptyList();
        this.options = options != null ? options : new java.util.HashMap<>();
        this.lastReadSnapshotId = -1;
        
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
     * Set the row type to know how to convert Map records to RowData
     */
    public void setRowType(RowType rowType) {
        this.rowType = rowType;
    }

    @Override
    public void configure(Configuration parameters) {
        System.out.println("Configuring MarukoInputFormat for path: " + this.tablePath);
        
        try {
            // Create a minimal schema for TableStore - we'll use the rowType information if available
            // otherwise create a basic schema
            MessageType schema;
            if (rowType != null) {
                // Convert from Flink RowType to Parquet MessageType
                schema = createParquetSchemaFromRowType(rowType);
            } else {
                // Try to infer schema from the table path or use a more appropriate fallback
                // For now, we'll create an empty schema and let Maruko handle schema discovery if possible
                System.out.println("MarukoInputFormat: WARNING - rowType is null, schema inference needed from actual data");
                // We should not create a default "id" schema here as it will override the real schema
                // Instead, we should ensure rowType is properly passed from MarukoTableSource
                schema = createEmptySchemaAsFallback();
            }
            
            // Use the primary key fields that were passed in from the TableStoreTableSource
            List<String> partitionFields = Collections.emptyList(); // For this test, no partition keys
            
            System.out.println("Initializing TableStore for reading with primary keys: " + this.primaryKeyFields);
            
            this.tableStore = new TableStore(this.tablePath, this.primaryKeyFields, partitionFields, 2, schema, options);
            
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
            System.out.println("TableStoreInputFormat: WARNING - rowType is null, schema inference needed from actual data");
            return new MessageType("tablestore_schema",
                Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("id"));
        }
        
        System.out.println("TableStoreInputFormat: Creating Parquet schema from RowType with " + rowType.getFieldCount() + " fields: " + rowType.getFieldNames());
        
        // Build Parquet fields from Flink RowType
        java.util.List<org.apache.parquet.schema.Type> parquetFields = new java.util.ArrayList<>();
        List<org.apache.flink.table.types.logical.LogicalType> fieldTypes = rowType.getChildren();
        List<String> fieldNames = rowType.getFieldNames();
        
        for (int i = 0; i < Math.min(fieldTypes.size(), fieldNames.size()); i++) {
            String fieldName = fieldNames.get(i);

            org.apache.flink.table.types.logical.LogicalType logicalType = fieldTypes.get(i);
            
            System.out.println("TableStoreInputFormat: Converting field '" + fieldName + "' with type " + logicalType);
            org.apache.parquet.schema.Type parquetType = convertLogicalTypeToParquetType(fieldName, logicalType);
            if (parquetType != null) {
                System.out.println("TableStoreInputFormat: Successfully converted field '" + fieldName + "' to Parquet type: " + parquetType);
                parquetFields.add(parquetType);
            } else {
                System.out.println("TableStoreInputFormat: Failed to convert field '" + fieldName + "'");
            }
        }
        
        // If no fields were converted, create a minimal schema
        if (parquetFields.isEmpty()) {
            System.out.println("TableStoreInputFormat: WARNING - No fields were converted, creating empty schema");
            return new MessageType("tablestore_schema",
                Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("id"));
        }
        
        System.out.println("TableStoreInputFormat: Successfully created Parquet schema with " + parquetFields.size() + " fields");
        org.apache.parquet.schema.Type[] fieldsArray = parquetFields.toArray(new org.apache.parquet.schema.Type[0]);
        MessageType schema = new MessageType("tablestore_schema", fieldsArray);
        System.out.println("TableStoreInputFormat: Final schema: " + schema);
        return schema;
    }
    
    /**
     * Create an empty schema as a last resort fallback
     */
    private MessageType createEmptySchemaAsFallback() {
        // Instead of creating a hardcoded 'id' schema, we should handle this more gracefully
        // This is a placeholder that won't interfere with actual table schema
        System.out.println("TableStoreInputFormat: Creating minimal fallback schema (this should be avoided)");
        // We'll create a minimal schema with no fields to avoid overriding actual schema
        return new MessageType("tablestore_schema");
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
        
        System.out.println("Starting Maruko streaming reader...");
        System.out.println("Validating initialization: tableStore=" + (tableStore != null) + 
                          ", rowType=" + (rowType != null) + 
                          ", lastReadSnapshotId=" + lastReadSnapshotId);
        
        // If tableStore is null (which can happen in Flink's execution model), initialize it in run method
        if (tableStore == null) {
            System.out.println("maruko is null during execution, initializing now...");
            try {
                // Create a minimal schema for TableStore - we'll use the rowType information if available
                // otherwise handle gracefully without creating a default "id" schema
                MessageType schema;
                if (rowType != null) {
                    // Convert from Flink RowType to Parquet MessageType
                    schema = createParquetSchemaFromRowType(rowType);
                } else {
                    // Handle the case where rowType is null more gracefully
                    System.out.println("TableStoreInputFormat: WARNING - rowType is null in run() method, schema inference needed");
                    // Create empty schema instead of default "id" schema
                    schema = createEmptySchemaAsFallback();
                }
                
                // Use the primary key fields that were passed in from the MarukoTableSource
                List<String> partitionFields = Collections.emptyList(); // For this test, no partition keys
                
                System.out.println("Initializing Maruko in run() method for path: " + this.tablePath + 
                                 " with primary keys: " + this.primaryKeyFields);
                
                this.tableStore = new TableStore(this.tablePath, this.primaryKeyFields, partitionFields, 2, schema, options);
                
                // Initialize the last read snapshot ID to current latest
                this.lastReadSnapshotId = tableStore.getLastReadSnapshotId();
                
                System.out.println("Maruko re-initialized in run() method for reading from: " + this.tablePath + 
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
        
        // First, read and emit initial full snapshot data (for streaming job that needs to process existing data)
        System.out.println("Reading initial full snapshot data...");
        List<Map<String, Object>> initialData = tableStore.getInitialFullSnapshot();
        System.out.println("Found " + (initialData != null ? initialData.size() : 0) + " records in initial snapshot, emitting to stream...");
        
        if (initialData != null && !initialData.isEmpty()) {
            for (Map<String, Object> record : initialData) {
                if (!isRunning) {
                    break;
                }
                
                if (record != null) {
                    if (rowType == null) {
                        System.out.println("Warning: rowType is null, cannot convert initial snapshot record: " + record);
                    } else if (rowType.getFields().isEmpty()) {
                        System.out.println("Warning: rowType has no fields, cannot convert initial snapshot record: " + record);
                    } else {
                        try {
                            MapToRowDataConverter converter = new MapToRowDataConverter(rowType);
                            if (converter != null) {
                                RowData rowData = converter.convert(record);
                                if (rowData != null) {
                                    synchronized (ctx.getCheckpointLock()) {
                                        ctx.collect(rowData);
                                    }
                                } else {
                                    System.out.println("Warning: Converter returned null for initial snapshot record: " + record);
                                    System.out.println("  Record content: " + record);
                                    System.out.println("  RowType fields: " + rowType.getFieldNames());
                                }
                            } else {
                                System.out.println("Warning: MapToRowDataConverter is null for initial snapshot record: " + record);
                            }
                        } catch (Exception e) {
                            System.err.println("Error converting initial snapshot record: " + e.getMessage());
                            e.printStackTrace();
                            System.out.println("  Record content: " + record);
                            System.out.println("  RowType: " + rowType);
                        }
                    }
                } else {
                    System.out.println("Warning: initial snapshot record is null");
                }
            }
        }
        System.out.println("Completed emitting initial snapshot data, starting incremental change monitoring...");
        
        System.out.println("Monitoring for new data changes since snapshot ID: " + lastReadSnapshotId);
        
        try {
            while (isRunning) {
                // Check if tableStore is properly initialized
                if (tableStore == null) {
                    System.out.println("ERROR: tableStore became null during execution!");
                    break; // Exit the loop if tableStore is null
                }
                
                // Instead of reading all records each time, use the changelog mechanism
                // This will detect new changes since last read
                List<org.example.maruko.core.StreamChangeRecord> changeRecords = null;
                try {
                    changeRecords = tableStore.computeChangelogSinceLastRead();
                } catch (Exception e) {
                    System.err.println("Error computing changelog from TableStore: " + e.getMessage());
                    e.printStackTrace();
                    // Continue the loop even if there's an error
                }

                if (changeRecords != null && !changeRecords.isEmpty()) {
                    System.out.println("Detected " + changeRecords.size() + " changelog records, emitting to stream...");
                    
                    for (org.example.maruko.core.StreamChangeRecord changeRecord : changeRecords) {
                        if (!isRunning) {
                            break;
                        }

                        // Get the appropriate record based on change type
                        Map<String, Object> record = null;
                        switch (changeRecord.getChangeType()) {
                            case INSERT:
                                record = changeRecord.getNewValue();
                                System.out.println("Processing INSERT change");
                                break;
                            case UPDATE_BEFORE:
                                record = changeRecord.getOldValue();
                                System.out.println("Processing UPDATE_BEFORE change");
                                break;
                            case UPDATE_AFTER:
                                record = changeRecord.getNewValue();
                                System.out.println("Processing UPDATE_AFTER change");
                                break;
                            case DELETE:
                                record = changeRecord.getOldValue();
                                System.out.println("Processing DELETE change");
                                break;
                        }

                        // Convert Map<String, Object> to RowData
                        if (record != null) {
                            if (rowType == null) {
                                System.out.println("Warning: rowType is null, cannot convert record: " + record);
                            } else if (rowType.getFields().isEmpty()) {
                                System.out.println("Warning: rowType has no fields, cannot convert record: " + record);
                            } else {
                                try {
                                    MapToRowDataConverter converter = new MapToRowDataConverter(rowType);
                                    if (converter != null) {
                                        RowData rowData = converter.convert(record);
                                        if (rowData != null) {
                                            synchronized (ctx.getCheckpointLock()) {
                                                ctx.collect(rowData);
                                            }
                                        } else {
                                            System.out.println("Warning: Converter returned null for record: " + record);
                                            System.out.println("  Record content: " + record);
                                            System.out.println("  RowType fields: " + rowType.getFieldNames());
                                        }
                                    } else {
                                        System.out.println("Warning: MapToRowDataConverter is null for record: " + record);
                                    }
                                } catch (Exception e) {
                                    System.err.println("Error converting record: " + e.getMessage());
                                    e.printStackTrace();
                                    System.out.println("  Record content: " + record);
                                    System.out.println("  RowType: " + rowType);
                                }
                            }
                        } else {
                            System.out.println("Warning: record is null");
                        }
                    }
                } else {
                    System.out.println("No new changelog records detected in this polling cycle, continuing...");
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