package org.example.tablestore.flink.sql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.example.tablestore.flink.TableStoreInputFormat;

import java.util.List;
import java.util.Map;

/**
 * TableSource implementation for TableStore that integrates with Flink's Table API and SQL.
 * This source is designed exclusively for streaming operations with continuous unbounded data.
 */
public class TableStoreTableSource implements ScanTableSource {
    
    private final String tablePath;
    private final List<String> primaryKeyFields;
    private final List<String> partitionFields;
    private final DataType producedDataType;
    private final boolean isBounded;

    /**
     * Creates a TableStore table source for streaming operations only.
     * 
     * @param tablePath The path to the TableStore table
     * @param primaryKeyFields List of primary key field names
     * @param partitionFields List of partition field names  
     * @param producedDataType The data type produced by this source
     * @param isBounded Always false - this source only supports streaming mode
     */
    public TableStoreTableSource(String tablePath, List<String> primaryKeyFields, 
                                List<String> partitionFields, DataType producedDataType, 
                                boolean isBounded) {
        // Enforce streaming-only mode
        if (isBounded) {
            throw new IllegalArgumentException("TableStoreTableSource only supports streaming mode (unbounded). Batch mode (bounded) is not supported.");
        }
        
        this.tablePath = tablePath;
        this.primaryKeyFields = primaryKeyFields;
        this.partitionFields = partitionFields;
        this.producedDataType = producedDataType;
        this.isBounded = false; // Always false for streaming-only mode
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // For streaming operations, we return only INSERT mode 
        // This source is designed for streaming reads of TableStore data
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        // Create the InputFormat for reading data in streaming mode
        System.out.println("Creating TableStoreInputFormat for path: " + tablePath + " (streaming mode)");
        TableStoreInputFormat inputFormat = new TableStoreInputFormat(tablePath, primaryKeyFields);
        
        // Set the row type from the produced data type to ensure proper schema handling
        org.apache.flink.table.types.logical.RowType rowType = 
            (org.apache.flink.table.types.logical.RowType) this.producedDataType.getLogicalType();
        inputFormat.setRowType(rowType);
        
        // Always use unbounded (streaming) mode - this source is streaming-only
        return SourceFunctionProvider.of(inputFormat, false);
    }

    @Override
    public DynamicTableSource copy() {
        // Always create a copy with streaming mode enforced
        return new TableStoreTableSource(tablePath, primaryKeyFields, partitionFields, 
                                       producedDataType, false);
    }

    @Override
    public String asSummaryString() {
        return "TableStore Streaming Table Source (CONTINUOUS_UNBOUNDED)";
    }

    // Add getter methods for accessing the configuration
    public String getTablePath() {
        return tablePath;
    }

    public List<String> getPrimaryKeyFields() {
        return primaryKeyFields;
    }

    public List<String> getPartitionFields() {
        return partitionFields;
    }
    
    public DataType getProducedDataType() {
        return producedDataType;
    }
    
    public boolean isBounded() {
        return isBounded;
    }
}