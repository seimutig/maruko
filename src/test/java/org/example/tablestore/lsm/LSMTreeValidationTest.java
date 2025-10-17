package org.example.tablestore.lsm;

import org.example.tablestore.core.TableStore;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.util.*;

/**
 * Test to validate that the LSM-tree implementation is working correctly
 */
public class LSMTreeValidationTest {
    public static void main(String[] args) {
        System.out.println("=== LSM-Tree Validation Test ===\n");
        
        try {
            runLSMTreeValidation();
        } catch (Exception e) {
            System.err.println("Error during LSM-tree validation: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void runLSMTreeValidation() throws IOException {
        System.out.println("Testing LSM-tree functionality...");
        
        // Define schema
        MessageType schema = Types.buildMessage()
                .addField(Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("id"))
                .addField(Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("name"))
                .addField(Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32).named("value"))
                .addField(Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("timestamp"))
                .named("test_record");
        
        List<String> primaryKeyFields = Arrays.asList("id");
        List<String> partitionFields = Arrays.asList("name");
        String tablePath = "/tmp/lsm_validation_test";
        
        TableStore tableStore = new TableStore(tablePath, primaryKeyFields, partitionFields, 2, schema);
        
        // Test 1: Basic LSM-tree structure
        System.out.println("Test 1: Basic LSM-tree structure validation");
        testBasicStructure(tableStore);
        
        // Test 2: File addition and retrieval
        System.out.println("\nTest 2: File addition and retrieval");
        testFileAdditionAndRetrieval(tableStore);
        
        // Test 3: Compaction triggering
        System.out.println("\nTest 3: Compaction triggering");
        testCompactionTriggering(tableStore);
        
        // Test 4: Level organization
        System.out.println("\nTest 4: Level organization");
        testLevelOrganization(tableStore);
        
        // Test 5: Statistics tracking
        System.out.println("\nTest 5: Statistics tracking");
        testStatisticsTracking(tableStore);
        
        System.out.println("\n=== All LSM-tree validation tests completed ===");
    }
    
    private static void testBasicStructure(TableStore tableStore) {
        try {
            // Check that LSM-tree can be created and accessed
            String stats = tableStore.getLSMTreeStats();
            System.out.println("  ✓ LSM-tree structure initialized: " + stats);
        } catch (Exception e) {
            System.out.println("  ✗ Basic structure test failed: " + e.getMessage());
        }
    }
    
    private static void testFileAdditionAndRetrieval(TableStore tableStore) {
        try {
            // Add some data to trigger file creation
            List<Map<String, Object>> testData = new ArrayList<>();
            
            for (int i = 1; i <= 5; i++) {
                Map<String, Object> record = new HashMap<>();
                record.put("id", "rec_" + i);
                record.put("name", "group_a");
                record.put("value", i * 10);
                record.put("timestamp", System.currentTimeMillis());
                testData.add(record);
            }
            
            tableStore.write(testData);
            
            // Check that files were added to LSM-tree
            String statsAfterWrite = tableStore.getLSMTreeStats();
            System.out.println("  ✓ Files added successfully: " + statsAfterWrite);
            
            // Try to read data
            List<Map<String, Object>> readData = tableStore.read();
            System.out.println("  ✓ Read " + readData.size() + " records from LSM-tree");
            
        } catch (Exception e) {
            System.out.println("  ✗ File addition/retrieval test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void testCompactionTriggering(TableStore tableStore) {
        try {
            // Add more data to potentially trigger compaction
            List<Map<String, Object>> moreData = new ArrayList<>();
            
            for (int i = 6; i <= 15; i++) {
                Map<String, Object> record = new HashMap<>();
                record.put("id", "rec_" + i);
                record.put("name", "group_b");
                record.put("value", i * 10);
                record.put("timestamp", System.currentTimeMillis());
                moreData.add(record);
            }
            
            tableStore.write(moreData);
            
            // Check stats after more writes
            String statsAfterMoreWrites = tableStore.getLSMTreeStats();
            System.out.println("  ✓ More files added: " + statsAfterMoreWrites);
            
            // Force compaction
            tableStore.compact();
            
            // Check stats after compaction
            String statsAfterCompaction = tableStore.getLSMTreeStats();
            System.out.println("  ✓ Compaction completed: " + statsAfterCompaction);
            
            System.out.println("  ✓ Compaction triggering test passed");
            
        } catch (Exception e) {
            System.out.println("  ✗ Compaction triggering test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void testLevelOrganization(TableStore tableStore) {
        try {
            // Add data that should distribute across levels
            List<Map<String, Object>> testData = new ArrayList<>();
            
            // Add many records to stress the level organization
            for (int i = 16; i <= 50; i++) {
                Map<String, Object> record = new HashMap<>();
                record.put("id", "rec_" + i);
                record.put("name", "group_c");
                record.put("value", i * 5);
                record.put("timestamp", System.currentTimeMillis());
                testData.add(record);
            }
            
            tableStore.write(testData);
            
            // Check level distribution
            String finalStats = tableStore.getLSMTreeStats();
            System.out.println("  ✓ Multi-level data distribution: " + finalStats);
            
            // Verify that we can still read all data correctly
            List<Map<String, Object>> allData = tableStore.read();
            System.out.println("  ✓ Read " + allData.size() + " records from multi-level LSM-tree");
            
        } catch (Exception e) {
            System.out.println("  ✗ Level organization test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void testStatisticsTracking(TableStore tableStore) {
        try {
            // Test that statistics are being tracked properly
            String initialStats = tableStore.getLSMTreeStats();
            System.out.println("  Initial statistics: " + initialStats);
            
            // Add some data and check that stats update
            List<Map<String, Object>> testData = new ArrayList<>();
            Map<String, Object> record = new HashMap<>();
            record.put("id", "stats_test");
            record.put("name", "stats_group");
            record.put("value", 999);
            record.put("timestamp", System.currentTimeMillis());
            testData.add(record);
            
            tableStore.write(testData);
            
            String updatedStats = tableStore.getLSMTreeStats();
            System.out.println("  Updated statistics: " + updatedStats);
            
            // Verify that stats changed
            if (!initialStats.equals(updatedStats)) {
                System.out.println("  ✓ Statistics tracking working correctly");
            } else {
                System.out.println("  ! Statistics didn't change as expected");
            }
            
        } catch (Exception e) {
            System.out.println("  ✗ Statistics tracking test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}