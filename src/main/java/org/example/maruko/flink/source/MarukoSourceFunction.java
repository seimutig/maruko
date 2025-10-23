package org.example.maruko.flink.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.data.RowData;
import org.example.maruko.core.TableStore;
import org.example.maruko.core.StreamChangeRecord;
import org.example.maruko.core.StreamChangeType;
import org.example.maruko.flink.serializer.MapToRowDataConverter;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;

/**
 * Modern streaming SourceFunction for TableStore that supports continuous reading
 * This replaces the legacy InputFormat approach for better streaming query support
 */
public class MarukoSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {
    
    private final String tablePath;
    private final List<String> primaryKeyFields;
    private volatile boolean isRunning = true;
    private RowType rowType;
    private TableStore tableStore;
    private boolean isBounded = false; // Flag to determine if running in batch mode

    public MarukoSourceFunction(String tablePath, List<String> primaryKeyFields, boolean isBounded) {
        this.tablePath = tablePath;
        this.primaryKeyFields = primaryKeyFields;
        this.isBounded = isBounded;
    }
    
    // Legacy constructor for compatibility - defaults to streaming mode
    public MarukoSourceFunction(String tablePath, List<String> primaryKeyFields) {
        this(tablePath, primaryKeyFields, false); // Default to streaming mode
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // Initialize TableStore with defensive copy of critical parameters
        // to ensure no path mixing occurs in complex Flink scenarios
        String safeTablePath = this.tablePath; // Ensure we use the path stored in constructor
        List<String> safePrimaryKeyFields = new ArrayList<>(this.primaryKeyFields);
        List<String> partitionFields = java.util.Collections.emptyList();
        
        org.apache.parquet.schema.MessageType schema;
        if (rowType != null) {
            System.out.println("TableStoreSourceFunction.open(): Creating schema from rowType with " + rowType.getFieldCount() + " fields: " + rowType.getFieldNames());
            // Convert the Flink RowType to Parquet MessageType properly
            schema = createParquetSchemaFromRowType(rowType);
        } else {
            System.out.println("TableStoreSourceFunction.open(): WARNING - rowType is null, using default schema!");
            schema = new org.apache.parquet.schema.MessageType("tablestore_schema",
                new org.apache.parquet.schema.PrimitiveType(
                    org.apache.parquet.schema.Type.Repetition.REQUIRED,
                    org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY, "id"));
        }
        
        this.tableStore = new TableStore(safeTablePath, safePrimaryKeyFields, partitionFields, 2, schema);
        
        // Double-check that the TableStore was initialized with the correct path
        if (!this.tableStore.getTablePath().equals(safeTablePath)) {
            System.err.println("ERROR: TableStoreSourceFunction path mismatch! Expected: " + 
                safeTablePath + ", but TableStore has: " + this.tableStore.getTablePath());
        }
    }
    
    /**
     * Create a Parquet schema from the Flink RowType
     */
    private org.apache.parquet.schema.MessageType createParquetSchemaFromRowType(org.apache.flink.table.types.logical.RowType flinkRowType) {
        // Build Parquet fields from Flink RowType
        List<org.apache.parquet.schema.Type> parquetFields = new ArrayList<>();
        List<org.apache.flink.table.types.logical.LogicalType> fieldTypes = flinkRowType.getChildren();
        List<String> fieldNames = flinkRowType.getFieldNames();
        
        for (int i = 0; i < Math.min(fieldTypes.size(), fieldNames.size()); i++) {
            String fieldName = fieldNames.get(i);
            org.apache.flink.table.types.logical.LogicalType logicalType = fieldTypes.get(i);
            org.apache.parquet.schema.Type.Repetition repetition = logicalType.isNullable() ? 
                org.apache.parquet.schema.Type.Repetition.OPTIONAL : 
                org.apache.parquet.schema.Type.Repetition.REQUIRED;
                
            org.apache.parquet.schema.Type parquetType = convertLogicalTypeToParquetType(fieldName, logicalType, repetition);
            if (parquetType != null) {
                parquetFields.add(parquetType);
                System.out.println("TableStoreSourceFunction: Added field '" + fieldName + "' with parquet type to schema");
            }
        }
        
        // If no fields were converted, create a minimal schema
        if (parquetFields.isEmpty()) {
            System.out.println("TableStoreSourceFunction: WARNING - No fields were converted, falling back to default schema with single 'id' field");
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
            System.out.println("TableStoreSourceFunction: Converting unknown type " + logicalType + " for field " + fieldName + " to INT64");
            return new org.apache.parquet.schema.PrimitiveType(repetition, 
                org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64, fieldName);
        }
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        long lastSnapshotId = -1;
        
        if (isBounded) {
            // Batch mode: Read all existing records once and then terminate
            System.out.println("TableStoreSourceFunction: Running in BATCH mode - reading all existing records");
            System.out.println("TableStoreSourceFunction: Table path is " + tablePath);
            
            // Read all existing data from TableStore (in a real implementation, this would read the full snapshot)
            List<Map<String, Object>> fullSnapshot = tableStore.read();
            System.out.println("TableStoreSourceFunction: Read " + fullSnapshot.size() + " records from tablestore in batch mode");
            
            if (fullSnapshot != null && !fullSnapshot.isEmpty()) {
                synchronized (ctx.getCheckpointLock()) {
                    for (Map<String, Object> record : fullSnapshot) {
                        if (record != null && rowType != null) {
                            MapToRowDataConverter converter = 
                                new MapToRowDataConverter(rowType);
                            RowData rowData = converter.convert(record);
                            if (rowData != null) {
                                ctx.collect(rowData);
                            }
                        }
                    }
                }
            }
            
            System.out.println("TableStoreSourceFunction: Batch read completed, terminating");
        } else {
            // Streaming mode: Implement incremental snapshot algorithm
            System.out.println("TableStoreSourceFunction: Running in STREAMING mode - implementing incremental snapshot algorithm");
            
            // First, read and emit all existing data from the table (initial snapshot)
            System.out.println("TableStoreSourceFunction: Reading and emitting initial snapshot data...");
            System.out.println("TableStoreSourceFunction: Table path is " + tablePath);
            
            List<Map<String, Object>> initialData = tableStore.read();
            System.out.println("TableStoreSourceFunction: Found " + initialData.size() + " records in initial snapshot");
            
            // Emit all initial records to the sink
            if (initialData != null && !initialData.isEmpty()) {
                synchronized (ctx.getCheckpointLock()) {
                    for (Map<String, Object> record : initialData) {
                        if (record != null && rowType != null) {
                            MapToRowDataConverter converter = 
                                new MapToRowDataConverter(rowType);
                            RowData rowData = converter.convert(record);
                            if (rowData != null) {
                                System.out.println("TableStoreSourceFunction: Emitting initial record to downstream: " + record);
                                ctx.collect(rowData);
                            } else {
                                System.out.println("TableStoreSourceFunction: Failed to convert initial record to RowData: " + record);
                            }
                        } else {
                            if (record == null) {
                                System.out.println("TableStoreSourceFunction: Skip emitting - record is null");
                            }
                            if (rowType == null) {
                                System.out.println("TableStoreSourceFunction: Skip emitting - rowType is null");
                            }
                        }
                    }
                }
            } else {
                System.out.println("TableStoreSourceFunction: No initial data found or initial data is null");
            }
            System.out.println("TableStoreSourceFunction: Completed emitting initial snapshot data");
            
            // Initialize by reading the current state from the last snapshot for change tracking
            org.example.maruko.format.Snapshot latestSnapshot = tableStore.getSnapshotManager().getLatestSnapshot();
            if (latestSnapshot != null) {
                lastSnapshotId = latestSnapshot.getId();
                System.out.println("TableStoreSourceFunction: Initialized change tracking with latest snapshot ID: " + lastSnapshotId);
            } else {
                System.out.println("TableStoreSourceFunction: No existing snapshots found for change tracking, will wait for new snapshots");
            }
            
            // Continuous process: read next snapshots and compute changelog
            while (isRunning) {
                try {
                    // Read changelog records based on differences between snapshots
                    List<org.example.maruko.core.StreamChangeRecord> changeRecords = tableStore.computeChangelogSinceLastRead();
                    
                    if (!changeRecords.isEmpty()) {
                        System.out.println("TableStoreSourceFunction: Found " + changeRecords.size() + " change records to emit");
                        
                        // Acquire lock before emitting records
                        synchronized (ctx.getCheckpointLock()) {
                            for (org.example.maruko.core.StreamChangeRecord change : changeRecords) {
                                Map<String, Object> record = null;
                                switch (change.getChangeType()) {
                                    case INSERT:
                                        record = change.getNewValue();
                                        System.out.println("TableStoreSourceFunction: Processing INSERT change");
                                        break;
                                    case UPDATE_BEFORE:
                                        // For proper changelog semantics, UPDATE_BEFORE represents the old state
                                        // that is being replaced. We emit this as a record to maintain full changelog.
                                        record = change.getOldValue();
                                        System.out.println("TableStoreSourceFunction: Processing UPDATE_BEFORE change (old state)");
                                        break;
                                    case UPDATE_AFTER:
                                        record = change.getNewValue();
                                        System.out.println("TableStoreSourceFunction: Processing UPDATE_AFTER change (new state)");
                                        break;
                                    case DELETE:
                                        record = change.getOldValue();
                                        System.out.println("TableStoreSourceFunction: Processing DELETE change");
                                        break;
                                }
                                
                                if (record != null && rowType != null) {
                                    MapToRowDataConverter converter = 
                                        new MapToRowDataConverter(rowType);
                                    RowData rowData = converter.convert(record);
                                    if (rowData != null) {
                                        System.out.println("TableStoreSourceFunction: Emitting change record to downstream: " + record);
                                        ctx.collect(rowData);
                                    } else {
                                        System.out.println("TableStoreSourceFunction: Failed to convert change record to RowData: " + record);
                                    }
                                } else {
                                    if (record == null) {
                                        System.out.println("TableStoreSourceFunction: Change record is null, skipping");
                                    }
                                    if (rowType == null) {
                                        System.out.println("TableStoreSourceFunction: rowType is null, cannot convert change record");
                                    }
                                }
                            }
                        }
                    } 
                    // else {
                    //     System.out.println("TableStoreSourceFunction: No change records found, waiting for new snapshots...");
                    // }
                    
                    // Sleep briefly to avoid busy waiting
                    Thread.sleep(1000); // 1 second polling interval
                } catch (Exception e) {
                    System.err.println("TableStoreSourceFunction: Error during snapshot reading: " + e.getMessage());
                    e.printStackTrace();
                    Thread.sleep(2000); // Wait longer if there's an error
                }
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    // Method to set the row type from outside
    public void setRowType(RowType rowType) {
        this.rowType = rowType;
        if (rowType != null) {
            System.out.println("TableStoreSourceFunction: RowType set with " + rowType.getFieldCount() + " fields: " + rowType.getFieldNames());
        } else {
            System.out.println("TableStoreSourceFunction: WARNING - RowType set to null!");
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        if (rowType != null) {
            return org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(rowType);
        }
        // Return a minimal RowType to avoid null issues
        return org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(
            org.apache.flink.table.types.logical.RowType.of());
    }
}