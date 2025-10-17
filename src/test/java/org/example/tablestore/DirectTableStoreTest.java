package org.example.tablestore;

import org.example.tablestore.core.TableStore;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.util.*;

/**
 * Direct test of the TableStore read functionality to verify the issue
 */
public class DirectTableStoreTest {

    public static void main(String[] args) {
        System.out.println("=== Direct TableStore Read Test ===");
        
        String tablePath = "/tmp/direct_tablestore_test/data/test_direct_table";
        try {
            // Create the same schema as used in the Flink test
            MessageType schema = new MessageType("tablestore_schema",
                Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("id"),
                Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("name"),
                Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32).named("age")
            );
            
            // Create TableStore instance
            List<String> primaryKeyFields = Arrays.asList("id");
            List<String> partitionFields = new ArrayList<>();
            
            TableStore tableStore = new TableStore(tablePath, primaryKeyFields, partitionFields, 2, schema);
            
            // Write some test data
            System.out.println("\n1. Writing test data...");
            List<Map<String, Object>> testData = new ArrayList<>();
            Map<String, Object> record1 = new HashMap<>();
            record1.put("id", 1L);
            record1.put("name", "Alice");
            record1.put("age", 25);
            record1.put("_sequence_number", System.currentTimeMillis());
            testData.add(record1);
            
            Map<String, Object> record2 = new HashMap<>();
            record2.put("id", 2L);
            record2.put("name", "Bob");
            record2.put("age", 30);
            record2.put("_sequence_number", System.currentTimeMillis() + 1);
            testData.add(record2);
            
            Map<String, Object> record3 = new HashMap<>();
            record3.put("id", 3L);
            record3.put("name", "Charlie");
            record3.put("age", 35);
            record3.put("_sequence_number", System.currentTimeMillis() + 2);
            testData.add(record3);
            
            tableStore.write(testData);
            System.out.println("✓ Test data written successfully");
            
            // Check the data directory
            System.out.println("\n2. Verifying data directory...");
            java.nio.file.Path dataPath = java.nio.file.Paths.get(tablePath);
            System.out.println("Data directory exists: " + java.nio.file.Files.exists(dataPath));
            
            if (java.nio.file.Files.exists(dataPath)) {
                System.out.println("Files in data directory:");
                java.nio.file.Files.walk(dataPath)
                    .filter(java.nio.file.Files::isRegularFile)
                    .forEach(file -> System.out.println("  - " + file.getFileName()));
            }
            
            // Try reading the data back
            System.out.println("\n3. Reading data back from TableStore...");
            List<Map<String, Object>> readData = tableStore.read();
            System.out.println("Number of records read: " + readData.size());
            
            for (int i = 0; i < readData.size(); i++) {
                System.out.println("  Record " + (i+1) + ": " + readData.get(i));
            }
            
            if (readData.isEmpty()) {
                System.out.println("\n❌ ISSUE CONFIRMED: TableStore.read() returned no results!");
            } else {
                System.out.println("\n✓ SUCCESS: TableStore.read() returned " + readData.size() + " records");
            }
            
            // Close resources
            tableStore.close();
            
        } catch (Exception e) {
            System.err.println("Error during direct tablestore test: " + e.getMessage());
            e.printStackTrace();
        }
    }
}