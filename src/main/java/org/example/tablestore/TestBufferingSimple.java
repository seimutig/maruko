package org.example.tablestore;

import org.example.tablestore.core.TableStore;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.util.*;

public class TestBufferingSimple {
    public static void main(String[] args) {
        try {
            System.out.println("=== Simple Buffering Test ===");
            
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
            String tablePath = "/tmp/test_buffering_simple";
            
            // Clean up previous test data
            java.nio.file.Path path = java.nio.file.Paths.get(tablePath);
            if (java.nio.file.Files.exists(path)) {
                java.nio.file.Files.walk(path)
                    .sorted(java.util.Comparator.reverseOrder())
                    .map(java.nio.file.Path::toFile)
                    .forEach(java.io.File::delete);
            }
            
            TableStore tableStore = new TableStore(tablePath, primaryKeyFields, partitionFields, 4, schema);
            
            System.out.println("Writing 10 individual records to test buffering...");
            
            // Write 10 individual records (should accumulate in buffer)
            for (int i = 1; i <= 10; i++) {
                List<Map<String, Object>> singleRecord = new ArrayList<>();
                Map<String, Object> record = new HashMap<>();
                record.put("id", "user_" + i);
                record.put("name", "User " + i);
                record.put("timestamp", (long) (i * 1000));
                singleRecord.add(record);
                
                System.out.println("Writing record " + i + ": user_" + i);
                tableStore.write(singleRecord);
            }
            
            System.out.println("\nTaking snapshot/checkpoint (should flush buffer)...");
            tableStore.checkpoint();
            
            System.out.println("\nReading data back...");
            List<Map<String, Object>> readData = tableStore.read();
            
            System.out.println("Read " + readData.size() + " records:");
            for (Map<String, Object> record : readData) {
                System.out.println("  " + record.get("id") + " - " + record.get("name"));
            }
            
            System.out.println("\n=== Buffering test completed! ===");
            
            // Close the table store
            tableStore.close();
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}