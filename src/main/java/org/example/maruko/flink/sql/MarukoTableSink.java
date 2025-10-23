package org.example.maruko.flink.sql;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.example.maruko.flink.MarukoOutputFormat;

import java.util.List;
import java.util.Map;

/**
 * TableSink implementation for Maruko that integrates with Flink's Table API and SQL.
 */
public class MarukoTableSink implements DynamicTableSink {
    
    private final String tablePath;
    private final List<String> primaryKeyFields;
    private final List<String> partitionFields;
    private final int numBuckets;
    private final DataType consumedDataType;
    private final Map<String, String> options;

    public MarukoTableSink(String tablePath, List<String> primaryKeyFields, 
                              List<String> partitionFields, int numBuckets, DataType consumedDataType, Map<String, String> options) {
        this.tablePath = tablePath;
        this.primaryKeyFields = primaryKeyFields;
        this.partitionFields = partitionFields;
        this.numBuckets = numBuckets;
        this.consumedDataType = consumedDataType;
        this.options = options != null ? options : new java.util.HashMap<>();
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // For deduplication, we can handle all change kinds
        return ChangelogMode.all();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        // Create the OutputFormat for writing data
        MarukoOutputFormat outputFormat = new MarukoOutputFormat(
            tablePath, primaryKeyFields, partitionFields, numBuckets, options);
        
        // Set the row type to know how to convert RowData records to Map
        outputFormat.setRowType((org.apache.flink.table.types.logical.RowType) consumedDataType.getLogicalType());
        
        return SinkFunctionProvider.of(outputFormat);
    }

    @Override
    public DynamicTableSink copy() {
        return new MarukoTableSink(tablePath, primaryKeyFields, partitionFields,
                                     numBuckets, consumedDataType, options);
    }

    @Override
    public String asSummaryString() {
        return "Maruko Table Sink";
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