package org.example.tablestore.flink.sql;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.*;

/**
 * Factory for creating TableStore table sources and sinks.
 * This connects TableStore with Flink's Table API and SQL.
 */
public class TableStoreDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "tablestore";

    public static final ConfigOption<String> TABLE_PATH = ConfigOptions
            .key("table-path")
            .stringType()
            .noDefaultValue()
            .withDescription("The path to the TableStore table");

    public static final ConfigOption<String> PRIMARY_KEYS = ConfigOptions
            .key("primary-keys")
            .stringType()
            .noDefaultValue()
            .withDescription("Comma-separated list of primary key fields");

    public static final ConfigOption<String> PARTITION_KEYS = ConfigOptions
            .key("partition-keys")
            .stringType()
            .noDefaultValue()
            .withDescription("Comma-separated list of partition key fields");

    public static final ConfigOption<Integer> NUM_BUCKETS = ConfigOptions
            .key("num-buckets")
            .intType()
            .defaultValue(4)
            .withDescription("Number of buckets for data distribution");

    

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TABLE_PATH);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PRIMARY_KEYS);
        options.add(PARTITION_KEYS);
        options.add(NUM_BUCKETS);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // Extract configuration from catalog table options
        Map<String, String> options = context.getCatalogTable().getOptions();
        
        String tablePath = options.get(TABLE_PATH.key());
        String primaryKeyStr = options.get(PRIMARY_KEYS.key());
        String partitionKeyStr = options.get(PARTITION_KEYS.key());
        
        // Parse primary keys
        List<String> primaryKeyFields = new ArrayList<>();
        if (primaryKeyStr != null && !primaryKeyStr.trim().isEmpty()) {
            String[] keys = primaryKeyStr.split(",");
            for (String key : keys) {
                primaryKeyFields.add(key.trim());
            }
        }
        
        // Parse partition keys
        List<String> partitionFields = new ArrayList<>();
        if (partitionKeyStr != null && !partitionKeyStr.trim().isEmpty()) {
            String[] keys = partitionKeyStr.split(",");
            for (String key : keys) {
                partitionFields.add(key.trim());
            }
        }
        
        // Hardcode to streaming mode only (unbounded = false)
        boolean isBounded = false;
        
        return new TableStoreTableSource(
            tablePath,
            primaryKeyFields,
            partitionFields,
            context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType(),
            isBounded
        );
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        // Extract configuration from catalog table options
        Map<String, String> options = context.getCatalogTable().getOptions();
        
        String tablePath = options.get(TABLE_PATH.key());
        String primaryKeyStr = options.get(PRIMARY_KEYS.key());
        String partitionKeyStr = options.get(PARTITION_KEYS.key());
        int numBuckets = 4; // Default value
        if (options.containsKey(NUM_BUCKETS.key())) {
            try {
                numBuckets = Integer.parseInt(options.get(NUM_BUCKETS.key()));
            } catch (NumberFormatException e) {
                System.out.println("Warning: Invalid num-buckets value, using default 4");
            }
        }
        
        // Parse primary keys
        List<String> primaryKeyFields = new ArrayList<>();
        if (primaryKeyStr != null && !primaryKeyStr.trim().isEmpty()) {
            String[] keys = primaryKeyStr.split(",");
            for (String key : keys) {
                primaryKeyFields.add(key.trim());
            }
        }
        
        // Parse partition keys
        List<String> partitionFields = new ArrayList<>();
        if (partitionKeyStr != null && !partitionKeyStr.trim().isEmpty()) {
            String[] keys = partitionKeyStr.split(",");
            for (String key : keys) {
                partitionFields.add(key.trim());
            }
        }
        
        return new TableStoreTableSink(
            tablePath,
            primaryKeyFields,
            partitionFields,
            numBuckets,
            context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType()
        );
    }
}