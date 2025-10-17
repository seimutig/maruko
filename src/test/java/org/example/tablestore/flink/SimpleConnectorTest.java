package org.example.tablestore.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Simple test to verify TableStore Flink connector is working
 */
public class SimpleConnectorTest {
    
    public static void main(String[] args) throws Exception {
        System.out.println("=== Simple TableStore Flink Connector Test ===");
        
        // Create environments
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        // Test basic setup
        String warehousePath = "/tmp/simple_connector_test";
        
        // Clean up previous test data
        cleanupTestData(warehousePath);
        
        // Register catalog
        TableStoreCatalog catalog = new TableStoreCatalog("test_catalog", warehousePath);
        tableEnv.registerCatalog("test_catalog", catalog);
        tableEnv.useCatalog("test_catalog");
        
        // Create database
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS test_db");
        tableEnv.useDatabase("test_db");
        
        // Create table
        tableEnv.executeSql(
            "CREATE TABLE employees (" +
            "  id STRING," +
            "  name STRING," +
            "  age INT," +
            "  salary DOUBLE," +
            "  department STRING," +
            "  PRIMARY KEY (id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'tablestore'," +
            "  'table-path' = '" + warehousePath + "/employees'," +
            "  'primary-keys' = 'id'" +
            ")"
        ).await();
        
        System.out.println("✓ TableStore connector registered and table created successfully");
        
        // Test data insertion
        tableEnv.executeSql(
            "INSERT INTO employees VALUES " +
            "('emp_001', 'Alice', 25, 75000.0, 'Engineering')"
        ).await();
        
        System.out.println("✓ Data insertion successful");
        
        // Test data querying
        tableEnv.executeSql("SELECT * FROM employees").print();
        
        System.out.println("✓ Data querying successful");
        System.out.println("=== Simple Connector Test Completed Successfully ===");
    }
    
    private static void cleanupTestData(String warehousePath) {
        try {
            java.io.File warehouseDir = new java.io.File(warehousePath);
            if (warehouseDir.exists()) {
                deleteRecursively(warehouseDir);
            }
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }
    
    private static void deleteRecursively(java.io.File file) {
        if (file.isDirectory()) {
            java.io.File[] files = file.listFiles();
            if (files != null) {
                for (java.io.File subFile : files) {
                    deleteRecursively(subFile);
                }
            }
        }
        file.delete();
    }
}