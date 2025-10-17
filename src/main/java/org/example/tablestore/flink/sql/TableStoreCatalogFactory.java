package org.example.tablestore.flink.sql;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Factory for creating TableStoreCatalog instances.
 */
public class TableStoreCatalogFactory implements CatalogFactory {

    public static final String IDENTIFIER = "tablestore";
    
    // Define configuration options
    public static final ConfigOption<String> WAREHOUSE_PATH = ConfigOptions
            .key("warehouse")
            .stringType()
            .defaultValue("/tmp/tablestore_warehouse")
            .withDescription("The path to the warehouse directory for TableStore");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(WAREHOUSE_PATH);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        // Get warehouse path from configuration
        String warehousePath = context.getOptions().get(WAREHOUSE_PATH);
        String catalogName = context.getName();
        
        return new TableStoreCatalog(catalogName, warehousePath);
    }
}