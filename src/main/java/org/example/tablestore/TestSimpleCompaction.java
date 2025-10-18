package org.example.tablestore;

import org.example.tablestore.core.TableStore;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.util.*;

public class TestSimpleCompaction {
    public static void main(String[] args) {
        try {
            System.out.println("Testing Compaction Behavior...");
            
            // Define schema
            MessageType schema = Types.buildMessage()
                    .addField(Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("id"))
                    .addField(Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("name"))
                    .addField(Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("timestamp"))
                    .named("test_record");
            
            // Define primary key and partition fields
            List<String> primaryKeyFields = Arrays.asList("id");
            List<String> partitionFields = Arrays.asList(); // No partitioning
            
            // Create table store instance
            String tablePath = "/tmp/test_simple_compaction";
            
            // Clean up previous test data
            java.nio.file.Path path = java.nio.file.Paths.get(tablePath);
            if (java.nio.file.Files.exists(path)) {
                java.nio.file.Files.walk(path)
                    .sorted(java.util.Comparator.reverseOrder())
                    .map(java.nio.file.Path::toFile)
                    .forEach(java.io.File::delete);
            }
            
            TableStore tableStore = new TableStore(tablePath, primaryKeyFields, partitionFields, 4, schema);
            
            System.out.println("Writing first batch of records...");
            
            // Write 4 separate records (each will be written as separate file)
            for (int i = 1; i <= 4; i++) {
                List<Map<String, Object>> singleRecord = new ArrayList<>();
                Map<String, Object> record = new HashMap<>();
                record.put("id", "user_" + i);
                record.put("name", "User " + i);
                record.put("timestamp", (long) (i * 1000));
                singleRecord.add(record);
                
                System.out.println("Writing record " + i + ": user_" + i);
                tableStore.write(singleRecord);
            }
            
            System.out.println("\nTaking snapshot after 4 writes...");
            tableStore.checkpoint();
            
            System.out.println("\nWriting additional record that should trigger compaction..."); 
            
            // Write one more record - this should trigger compaction since level0MaxFiles=4
            List<Map<String, Object>> fifthRecord = new ArrayList<>();
            Map<String, Object> record = new HashMap<>();
            record.put("id", "user_5");
            record.put("name", "User 5");
            record.put("timestamp", 5000L);
            fifthRecord.add(record);
            
            System.out.println("Writing record 5: user_5 (should trigger compaction)");
            tableStore.write(fifthRecord);
            
            System.out.println("\nReading data after compaction...");
            List<Map<String, Object>> readData = tableStore.read();
            
            System.out.println("Read " + readData.size() + " records:");
            for (Map<String, Object> recordData : readData) {
                System.out.println("  " + recordData.get("id") + " - " + recordData.get("name"));
            }
            
            System.out.println("\nSimple compaction test completed!");
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}