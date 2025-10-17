package org.example.tablestore.flink.sql;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.example.tablestore.flink.TableStoreOutputFormat;

import java.util.List;

/**
 * TableSink implementation for TableStore that integrates with Flink's Table API and SQL.
 */
public class TableStoreTableSink implements DynamicTableSink {
    
    private final String tablePath;
    private final List<String> primaryKeyFields;
    private final List<String> partitionFields;
    private final int numBuckets;
    private final DataType consumedDataType;

    public TableStoreTableSink(String tablePath, List<String> primaryKeyFields, 
                              List<String> partitionFields, int numBuckets, DataType consumedDataType) {
        this.tablePath = tablePath;
        this.primaryKeyFields = primaryKeyFields;
        this.partitionFields = partitionFields;
        this.numBuckets = numBuckets;
        this.consumedDataType = consumedDataType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // For deduplication, we can handle all change kinds
        return ChangelogMode.all();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        // Create the OutputFormat for writing data
        TableStoreOutputFormat outputFormat = new TableStoreOutputFormat(
            tablePath, primaryKeyFields, partitionFields, numBuckets);
        
        // Set the row type to know how to convert RowData records to Map
        outputFormat.setRowType((org.apache.flink.table.types.logical.RowType) consumedDataType.getLogicalType());
        
        return SinkFunctionProvider.of(outputFormat);
    }

    @Override
    public DynamicTableSink copy() {
        return new TableStoreTableSink(tablePath, primaryKeyFields, partitionFields, 
                                     numBuckets, consumedDataType);
    }

    @Override
    public String asSummaryString() {
        return "TableStore Table Sink";
    }

    // Getters for accessing configuration
    public String getTablePath() {
        return tablePath;
    }

    public List<String> getPrimaryKeyFields() {
        return primaryKeyFields;
    }

    public List<String> getPartitionFields() {
        return partitionFields;
    }

    public int getNumBuckets() {
        return numBuckets;
    }
    
    public DataType getConsumedDataType() {
        return consumedDataType;
    }
}