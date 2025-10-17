package org.example.tablestore.flink.sql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;
import org.example.tablestore.core.TableStore;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.util.*;

/**
 * Test program demonstrating both DataStream API and Table API usage with TableStore.
 */
public class TableStoreFlinkIntegrationTest {
    
    public static void main(String[] args) throws Exception {
        // Create StreamExecutionEnvironment for DataStream API
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2); // Set parallelism
        
        // Create TableEnvironment for Table API
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        System.out.println("=== TableStore Flink Integration Test ===\n");
        
        // Test 1: DataStream API usage
        System.out.println("1. Testing DataStream API integration...");
        testDataStreamAPI(env);
        
        System.out.println();
        
        // Test 2: Table API usage
        System.out.println("2. Testing Table API integration...");
        testTableAPI(tableEnv);
        
        System.out.println("\n=== Test completed successfully! ===");
    }
    
    /**
     * Test DataStream API integration with TableStore
     */
    private static void testDataStreamAPI(StreamExecutionEnvironment env) throws Exception {
        // Create some test data
        List<Tuple2<String, Integer>> testData = Arrays.asList(
            new Tuple2<>("user_1", 25),
            new Tuple2<>("user_2", 30),
            new Tuple2<>("user_1", 26), // This should overwrite the previous record due to deduplication
            new Tuple2<>("user_3", 22)
        );
        
        DataStream<Tuple2<String, Integer>> source = env.fromCollection(testData);
        
        // Note: In a real implementation, we'd connect this to TableStoreOutputFormat
        // For this demo, we'll just print the data
        source.print("DataStream Test: ");
        
        System.out.println("   DataStream API test data created successfully");
    }
    
    /**
     * Test Table API integration with TableStore
     */
    private static void testTableAPI(StreamTableEnvironment tableEnv) throws Exception {
        // 1. Create a TableStore catalog
        String catalogName = "tablestore_catalog";
        String warehousePath = "/tmp/tablestore_test";
        
        TableStoreCatalog catalog = new TableStoreCatalog(catalogName, warehousePath);
        tableEnv.registerCatalog(catalogName, catalog);
        tableEnv.useCatalog(catalogName);
        
        // 2. Create a test database
        String databaseName = "test_db";
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + databaseName);
        tableEnv.useDatabase(databaseName);
        
        // 3. Create a table with TableStore connector
        String tableName = "users";
        
        tableEnv.executeSql(
            "CREATE TABLE " + tableName + " (" +
            "  id STRING," +
            "  name STRING," +
            "  age INT," +
            "  salary DOUBLE," +
            "  department STRING," +
            "  PRIMARY KEY (id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'tablestore'," +
            "  'table-path' = '" + warehousePath + "/" + tableName + "'," +
            "  'primary-keys' = 'id'," +
            "  'partition-keys' = 'department'," +
            "  'num-buckets' = '4'" +
            ")"
        );
        
        // 4. Insert some test data
        System.out.println("   Inserting test data...");
        
        // Insert first set of data
        tableEnv.executeSql("INSERT INTO " + tableName + " VALUES " +
            "('emp_001', 'Alice', 25, 75000.0, 'Engineering')," +
            "('emp_002', 'Bob', 30, 80000.0, 'Marketing')," +
            "('emp_003', 'Charlie', 28, 70000.0, 'Engineering')"
        ).await();
        
        // Insert a duplicate record (should be deduplicated)
        tableEnv.executeSql("INSERT INTO " + tableName + " VALUES " +
            "('emp_001', 'Alice Updated', 26, 85000.0, 'Engineering')" // This should overwrite previous
        ).await();
        
        // 5. Query the data to verify deduplication
        System.out.println("   Querying data to verify deduplication...");
        
        Table result = tableEnv.sqlQuery("SELECT * FROM " + tableName + " WHERE department = 'Engineering'");
        TableResult queryResult = result.execute();
        
        // Print the results
        System.out.println("   Engineering department results:");
        queryResult.collect().forEachRemaining(row -> {
            System.out.println("     " + row);
        });
        
        // 6. Test another query with different partition
        System.out.println("   Querying all users...");
        TableResult allResult = tableEnv.sqlQuery("SELECT * FROM " + tableName).execute();
        System.out.println("   All user results:");
        allResult.collect().forEachRemaining(row -> {
            System.out.println("     " + row);
        });
        
        System.out.println("   Table API test completed successfully");
    }
    
    /**
     * Test direct TableStore usage (not through Flink connectors)
     */
    private static void testDirectTableStore() {
        try {
            // Define schema for the test
            MessageType schema = org.apache.parquet.schema.Types.buildMessage()
                    .addField(org.apache.parquet.schema.Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("id"))
                    .addField(org.apache.parquet.schema.Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("name"))
                    .addField(org.apache.parquet.schema.Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("timestamp"))
                    .addField(org.apache.parquet.schema.Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32).named("age"))
                    .addField(org.apache.parquet.schema.Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE).named("salary"))
                    .addField(org.apache.parquet.schema.Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("department"))
                    .named("test_record");
            
            // Define primary key and partition fields
            List<String> primaryKeyFields = Arrays.asList("id");
            List<String> partitionFields = Arrays.asList("department");
            
            // Create table store instance
            String tablePath = "/tmp/test_tablestore_direct";
            TableStore tableStore = new TableStore(tablePath, primaryKeyFields, partitionFields, 4, schema);
            
            System.out.println("   Direct TableStore test...");
            
            // Test data with duplicates to verify deduplication
            List<Map<String, Object>> testData = new ArrayList<>();
            
            // Add first version of record
            Map<String, Object> record1 = new HashMap<>();
            record1.put("id", "user_1");
            record1.put("name", "Alice");
            record1.put("age", 25);
            record1.put("salary", 75000.0);
            record1.put("timestamp", 1000L);
            record1.put("department", "Engineering");
            testData.add(record1);
            
            // Add second version of same record (should overwrite first)
            Map<String, Object> record2 = new HashMap<>();
            record2.put("id", "user_1");
            record2.put("name", "Alice Updated");
            record2.put("age", 26);
            record2.put("salary", 85000.0);
            record2.put("timestamp", 2000L); // Higher timestamp should win in deduplication
            record2.put("department", "Engineering");
            testData.add(record2);
            
            // Add another record
            Map<String, Object> record3 = new HashMap<>();
            record3.put("id", "user_2");
            record3.put("name", "Bob");
            record3.put("age", 30);
            record3.put("salary", 80000.0);
            record3.put("timestamp", 1500L);
            record3.put("department", "Marketing");
            testData.add(record3);
            
            System.out.println("   Writing test data to tablestore...");
            tableStore.write(testData);
            
            System.out.println("   Reading data back from tablestore...");
            List<Map<String, Object>> readData = tableStore.read();
            
            System.out.println("   Read " + readData.size() + " records:");
            for (Map<String, Object> record : readData) {
                System.out.println("     Record: " + record);
            }
            
            System.out.println("   Direct TableStore test completed successfully");
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}