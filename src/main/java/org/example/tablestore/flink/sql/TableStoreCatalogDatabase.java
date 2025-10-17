package org.example.tablestore.flink.sql;

import org.apache.flink.table.catalog.CatalogDatabase;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Simple implementation of CatalogDatabase for the TableStoreCatalog
 */
public class TableStoreCatalogDatabase implements CatalogDatabase {
    
    private final Map<String, String> properties;
    private final String comment;
    
    public TableStoreCatalogDatabase(Map<String, String> properties, String comment) {
        this.properties = properties != null ? properties : Collections.emptyMap();
        this.comment = comment;
    }
    
    @Override
    public Map<String, String> getProperties() {
        return properties;
    }
    
    @Override
    public String getComment() {
        return comment;
    }
    
    @Override
    public Optional<String> getDescription() {
        return Optional.ofNullable(comment);
    }
    
    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.ofNullable(comment);
    }
    
    @Override
    public CatalogDatabase copy() {
        return new TableStoreCatalogDatabase(properties, comment);
    }
    
    @Override
    public CatalogDatabase copy(Map<String, String> newProperties) {
        return new TableStoreCatalogDatabase(newProperties, comment);
    }
}