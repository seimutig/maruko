package org.example.tablestore;

import org.example.tablestore.core.TableStore;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.util.*;

public class TestSimpleBatchRead {
    public static void main(String[] args) {
        try {
            System.out.println("Testing Simple Batch Read Functionality...");
            
            // Define schema for the test
            MessageType schema = Types.buildMessage()
                    .addField(Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("id"))
                    .addField(Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("name"))
                    .addField(Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("timestamp"))
                    .addField(Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("department"))
                    .addField(Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32).named("age"))
                    .named("test_record");
            
            // Define primary key and partition fields
            List<String> primaryKeyFields = Arrays.asList("id");
            List<String> partitionFields = Arrays.asList("department");
            
            // Create table store instance for batch testing
            String tablePath = "/tmp/test_simple_batch_tablestore";
            // Clean up previous test data
            java.nio.file.Path path = java.nio.file.Paths.get(tablePath);
            if (java.nio.file.Files.exists(path)) {
                java.nio.file.Files.walk(path)
                    .sorted(java.util.Comparator.reverseOrder())
                    .map(java.nio.file.Path::toFile)
                    .forEach(java.io.File::delete);
            }
            
            TableStore tableStore = new TableStore(tablePath, primaryKeyFields, partitionFields, 4, schema);
            
            System.out.println("Testing batch read with sample data...");
            
            // Test data 
            List<Map<String, Object>> testData = new ArrayList<>();
            
            // Add some test records
            Map<String, Object> record1 = new HashMap<>();
            record1.put("id", "user_1");
            record1.put("name", "Alice");
            record1.put("age", 25);
            record1.put("timestamp", 1000L);
            record1.put("department", "engineering");
            testData.add(record1);
            
            Map<String, Object> record2 = new HashMap<>();
            record2.put("id", "user_2");
            record2.put("name", "Bob");
            record2.put("age", 30);
            record2.put("timestamp", 1500L);
            record2.put("department", "marketing");
            testData.add(record2);
            
            Map<String, Object> record3 = new HashMap<>();
            record3.put("id", "user_3");
            record3.put("name", "Charlie");
            record3.put("age", 28);
            record3.put("timestamp", 1200L);
            record3.put("department", "sales");
            testData.add(record3);
            
            System.out.println("Writing test data to tablestore...");
            tableStore.write(testData);
            
            // Create a snapshot without triggering compaction
            System.out.println("Creating snapshot...");
            tableStore.checkpoint();
            
            System.out.println("Reading data back from tablestore using batch read...");
            List<Map<String, Object>> readData = tableStore.read();
            
            System.out.println("Read " + readData.size() + " records via batch read:");
            for (Map<String, Object> record : readData) {
                System.out.println("  Record: " + extractKeyFields(record));
            }
            
            System.out.println("\nSimple batch read test completed successfully!");
            
            // Close the table store
            tableStore.close();
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    /** 
     * Helper method to extract key business fields for display purposes 
     */
    private static Map<String, Object> extractKeyFields(Map<String, Object> record) {
        Map<String, Object> keyFields = new HashMap<>();
        if (record != null) {
            for (Map.Entry<String, Object> entry : record.entrySet()) {
                // Skip metadata fields for cleaner display
                if (!entry.getKey().equals("_sequence_number") && !entry.getKey().equals("_version")) {
                    keyFields.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return keyFields;
    }
}