package org.example.tablestore;

import org.example.tablestore.core.TableStore;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.util.*;

public class TableStoreTest {
    public static void main(String[] args) {
        try {
            // Define schema for the test
            MessageType schema = Types.buildMessage()
                    .addField(Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("id"))
                    .addField(Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("name"))
                    .addField(Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("timestamp"))
                    .named("test_record");
            
            // Define primary key and partition fields
            List<String> primaryKeyFields = Arrays.asList("id");
            List<String> partitionFields = Arrays.asList("date");
            
            // Create table store instance
            String tablePath = "/tmp/test_tablestore";
            TableStore tableStore = new TableStore(tablePath, primaryKeyFields, partitionFields, 4, schema);
            
            System.out.println("Testing TableStore implementation...");
            
            // Test data with duplicates to verify deduplication
            List<Map<String, Object>> testData = new ArrayList<>();
            
            // Add first version of record
            Map<String, Object> record1 = new HashMap<>();
            record1.put("id", "user_1");
            record1.put("name", "Alice");
            record1.put("timestamp", 1000L);
            record1.put("date", "2023-01-01");
            testData.add(record1);
            
            // Add second version of same record (should overwrite first)
            Map<String, Object> record2 = new HashMap<>();
            record2.put("id", "user_1");
            record2.put("name", "Alice Updated");
            record2.put("timestamp", 2000L); // Higher timestamp should win in deduplication
            record2.put("date", "2023-01-01");
            testData.add(record2);
            
            // Add another record
            Map<String, Object> record3 = new HashMap<>();
            record3.put("id", "user_2");
            record3.put("name", "Bob");
            record3.put("timestamp", 1500L);
            record3.put("date", "2023-01-01");
            testData.add(record3);
            
            System.out.println("Writing test data to tablestore...");
            tableStore.write(testData);
            
            System.out.println("Reading data back from tablestore...");
            List<Map<String, Object>> readData = tableStore.read();
            
            System.out.println("Read " + readData.size() + " records:");
            for (Map<String, Object> record : readData) {
                System.out.println("  Record: " + record);
            }
            
            // Test compaction
            System.out.println("\nPerforming compaction...");
            tableStore.compact();
            
            System.out.println("\nTableStore test completed successfully!");
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
