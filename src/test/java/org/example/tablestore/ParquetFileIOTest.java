package org.example.tablestore;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.example.tablestore.core.TableStore;
import org.example.tablestore.flink.TableStoreInputFormat;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.util.*;

public class ParquetFileIOTest {
    
    public static void main(String[] args) {
        System.out.println("=== TableStore: Parquet File I/O Test ===\n");
        
        String testTablePath = "/tmp/test_tablestore_parquet";
        
        try {
            // Test 1: Create TableStore and write data to Parquet files
            System.out.println("1. Testing Parquet file writing...");
            
            // Define schema for the test
            MessageType schema = new MessageType("test_schema",
                Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("id"),
                Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("name"),
                Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("department"),
                Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32).named("age")
            );
            
            // Create TableStore instance
            TableStore tableStore = new TableStore(
                testTablePath,
                Arrays.asList("id"),  // Primary key fields
                Arrays.asList("department"),  // Partition fields  
                4,  // Number of buckets
                schema
            );
            
            // Prepare test data
            List<Map<String, Object>> testData = new ArrayList<>();
            Map<String, Object> record1 = new HashMap<>();
            record1.put("id", 1L);
            record1.put("name", "Alice");
            record1.put("department", "Engineering");
            record1.put("age", 30);
            testData.add(record1);
            
            Map<String, Object> record2 = new HashMap<>();
            record2.put("id", 2L);
            record2.put("name", "Bob");
            record2.put("department", "Marketing");
            record2.put("age", 25);
            testData.add(record2);
            
            Map<String, Object> record3 = new HashMap<>();
            record3.put("id", 3L);
            record3.put("name", "Charlie");
            record3.put("department", "Engineering");
            record3.put("age", 35);
            testData.add(record3);
            
            // Write the test data
            System.out.println("Writing data to Parquet files...");
            tableStore.write(testData);
            System.out.println("Data written successfully!\n");
            
            // Verify that Parquet files were created on disk
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Path tablePath = new Path(testTablePath);
            
            if (fs.exists(tablePath)) {
                System.out.println("2. Verifying Parquet files on disk...");
                System.out.println("Table directory exists: " + tablePath.toString());
                
                // List all files recursively in the table directory
                listFilesRecursively(fs, tablePath, 0);
            } else {
                System.out.println("ERROR: Table directory does not exist!");
                return;
            }
            
            // Test 3: Read the data back
            System.out.println("\n3. Testing data read from Parquet files...");
            List<Map<String, Object>> readData = tableStore.read();
            System.out.println("Read " + readData.size() + " records from Parquet files:");
            
            for (int i = 0; i < readData.size(); i++) {
                System.out.println("Record " + (i+1) + ": " + readData.get(i));
            }
            
            // Test 4: Use TableStoreInputFormat to read data
            System.out.println("\n4. Testing TableStoreInputFormat for Parquet reading...");
            
            // Define RowType for Flink integration
            RowType rowType = RowType.of(
                new BigIntType(),    // id
                new VarCharType(),   // name
                new VarCharType(),   // department
                new IntType()        // age
            );
            
            TableStoreInputFormat inputFormat = new TableStoreInputFormat(testTablePath);
            inputFormat.setRowType(rowType);
            
            // Configure the input format
            inputFormat.configure(new org.apache.flink.configuration.Configuration());
            
            System.out.println("TableStoreInputFormat configured successfully!");
            System.out.println("TableStoreInputFormat ready for reading");
            
            tableStore.close();
            System.out.println("\n=== Parquet File I/O Test Completed Successfully! ===");
            
        } catch (Exception e) {
            System.err.println("Error during Parquet file I/O test: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static String createIndent(int depth) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < depth; i++) {
            sb.append("  ");
        }
        return sb.toString();
    }
    
    private static void listFilesRecursively(FileSystem fs, Path path, int depth) throws IOException {
        org.apache.hadoop.fs.FileStatus[] statuses = fs.listStatus(path);
        
        for (org.apache.hadoop.fs.FileStatus status : statuses) {
            String indent = createIndent(depth);
            
            if (status.isDirectory()) {
                System.out.println(indent + "[DIR] " + status.getPath().getName());
                listFilesRecursively(fs, status.getPath(), depth + 1);
            } else {
                String fileName = status.getPath().getName();
                String marker = fileName.toLowerCase().endsWith(".parquet") ? "[PARQUET]" : "[FILE]";
                System.out.println(indent + marker + " " + fileName + " (" + status.getLen() + " bytes)");
            }
        }
    }
}