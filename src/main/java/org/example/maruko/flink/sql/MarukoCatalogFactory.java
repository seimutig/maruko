package org.example.maruko.flink.sql;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Factory for creating MarukoCatalog instances.
 */
public class MarukoCatalogFactory implements CatalogFactory {

    public static final String IDENTIFIER = "maruko";
    
    // Define configuration options
    public static final ConfigOption<String> WAREHOUSE_PATH = ConfigOptions
            .key("warehouse")
            .stringType()
            .defaultValue("/tmp/maruko_warehouse")
            .withDescription("The path to the warehouse directory for Maruko");

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

    public MarukoCatalog createCatalog(String name, Map<String, String> properties) {
        String warehousePath = properties.get(WAREHOUSE_PATH.key());
        if (warehousePath == null) {
            warehousePath = WAREHOUSE_PATH.defaultValue();
        }
        return new MarukoCatalog(name, warehousePath);
    }
}