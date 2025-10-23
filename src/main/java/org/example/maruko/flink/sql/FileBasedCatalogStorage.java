package org.example.maruko.flink.sql;

import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.factories.Factory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.*;
import java.util.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * A file-based implementation of catalog storage that persists metadata to disk
 * instead of keeping it in memory.
 */
public class FileBasedCatalogStorage {
    private final String catalogPath;
    private final ObjectMapper objectMapper;
    private final Map<String, CatalogDatabase> databases;
    private final Map<String, Map<String, CatalogBaseTable>> tables;
    private final Map<String, Map<String, Map<CatalogPartitionSpec, CatalogPartition>>> partitions;
    
    public FileBasedCatalogStorage(String warehousePath, String catalogName) {
        this.catalogPath = Paths.get(warehousePath, catalogName, "catalog_metadata").toString();
        this.objectMapper = new ObjectMapper();
        this.databases = new HashMap<>();
        this.tables = new HashMap<>();
        this.partitions = new HashMap<>();
        
        // Initialize directories if they don't exist
        ensureDirectories();
        
        // Load existing metadata from files
        loadMetadata();
    }
    
    private void ensureDirectories() {
        try {
            Files.createDirectories(Paths.get(catalogPath));
            Files.createDirectories(Paths.get(catalogPath, "databases"));
            Files.createDirectories(Paths.get(catalogPath, "tables"));
            Files.createDirectories(Paths.get(catalogPath, "partitions"));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create catalog directories: " + e.getMessage(), e);
        }
    }
    
    private void loadMetadata() {
        // Load databases
        loadDatabases();
        
        // Load tables
        loadTables();
        
        // Load partitions
        loadPartitions();
    }
    
    private void loadDatabases() {
        Path databasesDir = Paths.get(catalogPath, "databases");
        if (Files.exists(databasesDir)) {
            try {
                Files.list(databasesDir)
                    .filter(path -> path.toString().endsWith(".json"))
                    .forEach(this::loadDatabaseFromFile);
            } catch (IOException e) {
                System.err.println("Error loading databases: " + e.getMessage());
            }
        }
    }
    
    private void loadDatabaseFromFile(Path databaseFile) {
        try {
            String dbName = databaseFile.getFileName().toString().replace(".json", "");
            String content = new String(Files.readAllBytes(databaseFile));
            // Use a generic map approach since CatalogDatabaseImpl has complex structure
            @SuppressWarnings("unchecked")
            Map<String, Object> dbMap = objectMapper.readValue(content, Map.class);
            
            // Extract properties and description from the map
            Map<String, String> properties = (Map<String, String>) dbMap.get("properties");
            if (properties == null) {
                properties = new HashMap<>();
            }
            
            // Create new CatalogDatabaseImpl with the extracted data
            CatalogDatabase database = new CatalogDatabaseImpl(properties, (String) dbMap.get("comment"));
            databases.put(dbName, database);
        } catch (IOException e) {
            System.err.println("Error loading database from file: " + databaseFile + ", error: " + e.getMessage());
        }
    }
    
    private void loadTables() {
        Path tablesDir = Paths.get(catalogPath, "tables");
        if (Files.exists(tablesDir)) {
            try {
                Files.list(tablesDir)
                    .filter(Files::isDirectory)
                    .forEach(this::loadDatabaseTables);
            } catch (IOException e) {
                System.err.println("Error loading tables: " + e.getMessage());
            }
        }
    }
    
    private void loadDatabaseTables(Path databaseDir) {
        String dbName = databaseDir.getFileName().toString();
        try {
            Files.list(databaseDir)
                .filter(path -> path.toString().endsWith(".json"))
                .forEach(tableFile -> loadTableFromFile(dbName, tableFile));
                
            // Initialize the database's table map if not already present
            if (!tables.containsKey(dbName)) {
                tables.put(dbName, new HashMap<>());
            }
        } catch (IOException e) {
            System.err.println("Error loading tables for database: " + dbName + ", error: " + e.getMessage());
        }
    }
    
    private void loadTableFromFile(String dbName, Path tableFile) {
        try {
            String tableName = tableFile.getFileName().toString().replace(".json", "");
            String content = new String(Files.readAllBytes(tableFile));
            
            // For now, we'll handle the deserialization issue by creating a minimal table
            // since direct deserialization to CatalogTableImpl causes issues
            Map<String, String> options = new HashMap<>();
            String comment = null;
            
            // Try to parse the content to extract basic info without full deserialization
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> tableMap = objectMapper.readValue(content, Map.class);
                
                // Extract basic information for debugging purposes
                Object optionsObj = tableMap.get("options");
                Object commentObj = tableMap.get("comment");
                
                if (optionsObj instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> optionsMap = (Map<String, Object>) optionsObj;
                    // Convert values to strings
                    for (Map.Entry<String, Object> entry : optionsMap.entrySet()) {
                        options.put(entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : null);
                    }
                }
                
                if (commentObj != null) {
                    comment = commentObj.toString();
                }
            } catch (Exception e) {
                System.err.println("Could not parse table metadata, using defaults: " + e.getMessage());
            }
            
            // Create a minimal table schema - we'll use a simple approach to avoid complex deserialization
            // In a production implementation, we'd need to properly handle the schema serialization
            org.apache.flink.table.catalog.CatalogBaseTable minimalTable = 
                org.apache.flink.table.catalog.CatalogTable.of(
                    org.apache.flink.table.api.Schema.newBuilder().build(), // empty schema
                    comment != null ? comment : "",
                    new java.util.ArrayList<>(), // partition keys
                    options
                );
            
            tables.computeIfAbsent(dbName, k -> new HashMap<>()).put(tableName, minimalTable);
        } catch (Exception e) {
            System.err.println("Error loading table from file: " + tableFile + ", error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private void loadPartitions() {
        Path partitionsDir = Paths.get(catalogPath, "partitions");
        if (Files.exists(partitionsDir)) {
            try {
                Files.list(partitionsDir)
                    .filter(Files::isDirectory)
                    .forEach(this::loadDatabasePartitions);
            } catch (IOException e) {
                System.err.println("Error loading partitions: " + e.getMessage());
            }
        }
    }
    
    private void loadDatabasePartitions(Path databaseDir) {
        String dbName = databaseDir.getFileName().toString();
        try {
            Files.list(databaseDir)
                .filter(Files::isDirectory)
                .forEach(tableDir -> loadTablePartitions(dbName, tableDir));
                
            // Initialize the database's partition map if not already present
            if (!partitions.containsKey(dbName)) {
                partitions.put(dbName, new HashMap<>());
            }
        } catch (IOException e) {
            System.err.println("Error loading partitions for database: " + dbName + ", error: " + e.getMessage());
        }
    }
    
    private void loadTablePartitions(String dbName, Path tableDir) {
        String tableName = tableDir.getFileName().toString();
        try {
            Map<CatalogPartitionSpec, CatalogPartition> tablePartitions = new HashMap<>();
            
            Files.list(tableDir)
                .filter(path -> path.toString().endsWith(".json"))
                .forEach(partitionFile -> loadPartitionFromFile(tablePartitions, partitionFile));
                
            partitions.computeIfAbsent(dbName, k -> new HashMap<>()).put(tableName, tablePartitions);
        } catch (IOException e) {
            System.err.println("Error loading partitions for table: " + tableName + ", error: " + e.getMessage());
        }
    }
    
    private void loadPartitionFromFile(Map<CatalogPartitionSpec, CatalogPartition> tablePartitions, Path partitionFile) {
        try {
            String content = new String(Files.readAllBytes(partitionFile));
            // The partition file contains both the spec and the partition data
            // For simplicity, we'll just store it with a placeholder key
            // In a real implementation, we'd parse the content to extract the spec
        } catch (IOException e) {
            System.err.println("Error loading partition from file: " + partitionFile + ", error: " + e.getMessage());
        }
    }
    
    public void saveDatabase(String dbName, CatalogDatabase database) {
        try {
            Path dbFile = Paths.get(catalogPath, "databases", dbName + ".json");
            
            // Create a simplified structure that can be properly serialized/deserialized
            Map<String, Object> dbData = new HashMap<>();
            dbData.put("properties", database.getProperties());
            dbData.put("comment", database.getDescription().orElse(null));
            
            String json = objectMapper.writeValueAsString(dbData);
            Files.write(dbFile, json.getBytes());
            
            // Also update the in-memory cache
            databases.put(dbName, database);
        } catch (IOException e) {
            throw new RuntimeException("Failed to save database: " + dbName, e);
        }
    }
    
    public void deleteDatabase(String dbName) {
        try {
            Path dbFile = Paths.get(catalogPath, "databases", dbName + ".json");
            if (Files.exists(dbFile)) {
                Files.delete(dbFile);
            }
            // Also remove from in-memory cache
            databases.remove(dbName);
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete database: " + dbName, e);
        }
    }
    
    public void saveTable(String dbName, String tableName, CatalogBaseTable table) {
        try {
            Path dbTablesDir = Paths.get(catalogPath, "tables", dbName);
            Files.createDirectories(dbTablesDir);
            
            Path tableFile = Paths.get(dbTablesDir.toString(), tableName + ".json");
            String json = objectMapper.writeValueAsString(table);
            Files.write(tableFile, json.getBytes());
            
            // Also update the in-memory cache
            tables.computeIfAbsent(dbName, k -> new HashMap<>()).put(tableName, table);
        } catch (IOException e) {
            throw new RuntimeException("Failed to save table: " + dbName + "." + tableName, e);
        }
    }
    
    public void deleteTable(String dbName, String tableName) {
        try {
            Path tableFile = Paths.get(catalogPath, "tables", dbName, tableName + ".json");
            if (Files.exists(tableFile)) {
                Files.delete(tableFile);
            }
            // Also remove from in-memory cache
            Map<String, CatalogBaseTable> dbTables = tables.get(dbName);
            if (dbTables != null) {
                dbTables.remove(tableName);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete table: " + dbName + "." + tableName, e);
        }
    }
    
    public CatalogDatabase getDatabase(String dbName) {
        return databases.get(dbName);
    }
    
    public CatalogBaseTable getTable(String dbName, String tableName) {
        Map<String, CatalogBaseTable> dbTables = tables.get(dbName);
        if (dbTables != null) {
            return dbTables.get(tableName);
        }
        return null;
    }
    
    public Map<String, CatalogDatabase> getDatabases() {
        return new HashMap<>(databases);
    }
    
    public Map<String, CatalogBaseTable> getTables(String dbName) {
        Map<String, CatalogBaseTable> dbTables = tables.get(dbName);
        if (dbTables != null) {
            return new HashMap<>(dbTables);
        }
        return new HashMap<>();
    }
    
    public void clear() {
        databases.clear();
        tables.clear();
        partitions.clear();
    }
    
    public boolean databaseExists(String dbName) {
        return databases.containsKey(dbName);
    }
    
    public void savePartition(String dbName, String tableName, CatalogPartitionSpec partitionSpec, CatalogPartition partition) {
        // For simplicity, we'll store partitions in a file per table
        // In a real implementation, we'd have a more sophisticated approach
        try {
            Path dbPartitionsDir = Paths.get(catalogPath, "partitions", dbName);
            Files.createDirectories(dbPartitionsDir);
            
            Path tablePartitionsDir = Paths.get(dbPartitionsDir.toString(), tableName);
            Files.createDirectories(tablePartitionsDir);
            
            // Create a unique filename for the partition spec
            String partitionKey = generatePartitionKey(partitionSpec);
            Path partitionFile = Paths.get(tablePartitionsDir.toString(), partitionKey + ".json");
            
            // Store partition data
            Map<String, Object> partitionData = new HashMap<>();
            partitionData.put("spec", partitionSpec);
            partitionData.put("partition", partition);
            
            String json = objectMapper.writeValueAsString(partitionData);
            Files.write(partitionFile, json.getBytes());
        } catch (IOException e) {
            throw new RuntimeException("Failed to save partition: " + dbName + "." + tableName + "." + partitionSpec, e);
        }
    }
    
    public void deletePartition(String dbName, String tableName, CatalogPartitionSpec partitionSpec) {
        try {
            Path dbPartitionsDir = Paths.get(catalogPath, "partitions", dbName);
            if (!Files.exists(dbPartitionsDir)) return;
            
            Path tablePartitionsDir = Paths.get(dbPartitionsDir.toString(), tableName);
            if (!Files.exists(tablePartitionsDir)) return;
            
            String partitionKey = generatePartitionKey(partitionSpec);
            Path partitionFile = Paths.get(tablePartitionsDir.toString(), partitionKey + ".json");
            
            if (Files.exists(partitionFile)) {
                Files.delete(partitionFile);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete partition: " + dbName + "." + tableName + "." + partitionSpec, e);
        }
    }
    
    public CatalogPartition getPartition(String dbName, String tableName, CatalogPartitionSpec partitionSpec) {
        try {
            Path dbPartitionsDir = Paths.get(catalogPath, "partitions", dbName);
            if (!Files.exists(dbPartitionsDir)) return null;
            
            Path tablePartitionsDir = Paths.get(dbPartitionsDir.toString(), tableName);
            if (!Files.exists(tablePartitionsDir)) return null;
            
            String partitionKey = generatePartitionKey(partitionSpec);
            Path partitionFile = Paths.get(tablePartitionsDir.toString(), partitionKey + ".json");
            
            if (Files.exists(partitionFile)) {
                String content = new String(Files.readAllBytes(partitionFile));
                // Parse the partition data (simplified)
                return null; // For this implementation, we'll return null for now
            }
        } catch (IOException e) {
            System.err.println("Error loading partition: " + e.getMessage());
        }
        return null;
    }
    
    public boolean partitionExists(String dbName, String tableName, CatalogPartitionSpec partitionSpec) {
        try {
            Path dbPartitionsDir = Paths.get(catalogPath, "partitions", dbName);
            if (!Files.exists(dbPartitionsDir)) return false;
            
            Path tablePartitionsDir = Paths.get(dbPartitionsDir.toString(), tableName);
            if (!Files.exists(tablePartitionsDir)) return false;
            
            String partitionKey = generatePartitionKey(partitionSpec);
            Path partitionFile = Paths.get(tablePartitionsDir.toString(), partitionKey + ".json");
            
            return Files.exists(partitionFile);
        } catch (Exception e) {
            return false;
        }
    }
    
    private String generatePartitionKey(CatalogPartitionSpec partitionSpec) {
        // Create a unique key based on partition spec values
        StringBuilder key = new StringBuilder();
        for (Map.Entry<String, String> entry : partitionSpec.getPartitionSpec().entrySet()) {
            key.append(entry.getKey()).append("_").append(entry.getValue()).append("_");
        }
        return key.toString().replaceAll("[^a-zA-Z0-9_]", "_");
    }
    
    public boolean tableExists(String dbName, String tableName) {
        Map<String, CatalogBaseTable> dbTables = getTables(dbName);
        return dbTables.containsKey(tableName);
    }
}