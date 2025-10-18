package org.example.tablestore;

import org.example.tablestore.core.TableStore;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.util.*;

public class TestBatchRead {
    public static void main(String[] args) {
        try {
            System.out.println("Testing Batch Read Functionality...");
            
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
            String tablePath = "/tmp/test_batch_read_tablestore";
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
            
            // Test data with duplicates to verify deduplication
            List<Map<String, Object>> testData = new ArrayList<>();
            
            // Add first version of record
            Map<String, Object> record1 = new HashMap<>();
            record1.put("id", "user_1");
            record1.put("name", "Alice");
            record1.put("age", 25);
            record1.put("timestamp", 1000L);
            record1.put("department", "engineering");
            testData.add(record1);
            
            // Add second version of same record (should overwrite first due to deduplication)
            Map<String, Object> record2 = new HashMap<>();
            record2.put("id", "user_1");
            record2.put("name", "Alice Updated");
            record2.put("age", 26);
            record2.put("timestamp", 2000L); // Higher timestamp should win in deduplication
            record2.put("department", "engineering");
            testData.add(record2);
            
            // Add another record
            Map<String, Object> record3 = new HashMap<>();
            record3.put("id", "user_2");
            record3.put("name", "Bob");
            record3.put("age", 30);
            record3.put("timestamp", 1500L);
            record3.put("department", "marketing");
            testData.add(record3);
            
            // Add a record that will be updated later
            Map<String, Object> record4 = new HashMap<>();
            record4.put("id", "user_3");
            record4.put("name", "Charlie");
            record4.put("age", 28);
            record4.put("timestamp", 1200L);
            record4.put("department", "sales");
            testData.add(record4);
            
            System.out.println("Writing test data to tablestore...");
            tableStore.write(testData);
            
            // Create a checkpoint/snapshot
            System.out.println("Creating snapshot/checkpoint...");
            tableStore.checkpoint();
            
            // Add an update to test that it's captured in the next read
            List<Map<String, Object>> updateData = new ArrayList<>();
            Map<String, Object> updatedRecord = new HashMap<>();
            updatedRecord.put("id", "user_3");
            updatedRecord.put("name", "Charlie Updated");
            updatedRecord.put("age", 29);
            updatedRecord.put("timestamp", 2500L); // Even higher timestamp
            updatedRecord.put("department", "sales");
            updateData.add(updatedRecord);
            
            System.out.println("Writing update data to tablestore...");
            tableStore.write(updateData);
            
            // Create another checkpoint/snapshot
            System.out.println("Creating second snapshot/checkpoint...");
            tableStore.checkpoint();
            
            System.out.println("Reading data back from tablestore using batch read...");
            List<Map<String, Object>> readData = tableStore.read();
            
            System.out.println("Read " + readData.size() + " records via batch read:");
            for (Map<String, Object> record : readData) {
                System.out.println("  Record: " + extractKeyFields(record));
            }
            
            // Test compaction
            System.out.println("\nPerforming compaction...");
            tableStore.compact();
            
            System.out.println("Reading data back after compaction...");
            List<Map<String, Object>> readDataAfterCompaction = tableStore.read();
            
            System.out.println("Read " + readDataAfterCompaction.size() + " records after compaction:");
            for (Map<String, Object> record : readDataAfterCompaction) {
                System.out.println("  Record: " + extractKeyFields(record));
            }
            
            System.out.println("\nBatch read test completed successfully!");
            
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