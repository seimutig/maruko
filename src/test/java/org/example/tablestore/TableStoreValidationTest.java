package org.example.tablestore;

import org.example.tablestore.core.TableStore;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.util.*;

/**
 * Comprehensive test to validate all implemented functionality.
 */
public class TableStoreValidationTest {
    public static void main(String[] args) {
        System.out.println("=== TableStore Validation Tests ===\n");
        
        int passedTests = 0;
        int totalTests = 0;
        
        try {
            // Test 1: Basic functionality
            totalTests++;
            if (testBasicFunctionality()) {
                System.out.println("✓ Test 1 PASSED: Basic functionality");
                passedTests++;
            } else {
                System.out.println("✗ Test 1 FAILED: Basic functionality");
            }
            
            System.out.println();
            
            // Test 2: Deduplication
            totalTests++;
            if (testDeduplication()) {
                System.out.println("✓ Test 2 PASSED: Deduplication");
                passedTests++;
            } else {
                System.out.println("✗ Test 2 FAILED: Deduplication");
            }
            
            System.out.println();
            
            // Test 3: Partitioning
            totalTests++;
            if (testPartitioning()) {
                System.out.println("✓ Test 3 PASSED: Partitioning");
                passedTests++;
            } else {
                System.out.println("✗ Test 3 FAILED: Partitioning");
            }
            
            System.out.println();
            
            // Test 4: Error handling
            totalTests++;
            if (testErrorHandling()) {
                System.out.println("✓ Test 4 PASSED: Error handling");
                passedTests++;
            } else {
                System.out.println("✗ Test 4 FAILED: Error handling");
            }
            
            System.out.println();
            
            // Test 5: Schema validation
            totalTests++;
            if (testSchemaValidation()) {
                System.out.println("✓ Test 5 PASSED: Schema validation");
                passedTests++;
            } else {
                System.out.println("✗ Test 5 FAILED: Schema validation");
            }
            
            System.out.println();
            
            System.out.println("=== Test Results ===");
            System.out.println("Passed: " + passedTests + "/" + totalTests);
            System.out.println("Success Rate: " + (passedTests * 100 / totalTests) + "%");
            
            if (passedTests == totalTests) {
                System.out.println("🎉 All tests passed! TableStore is working correctly.");
            } else {
                System.out.println("⚠️  Some tests failed. Please review the implementation.");
            }
            
        } catch (Exception e) {
            System.err.println("Error during testing: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static boolean testBasicFunctionality() {
        try {
            System.out.println("Testing basic write/read functionality...");
            
            MessageType schema = Types.buildMessage()
                    .addField(Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("id"))
                    .addField(Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("name"))
                    .addField(Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32).named("age"))
                    .named("test_basic");
            
            List<String> primaryKeyFields = Arrays.asList("id");
            List<String> partitionFields = Arrays.asList("name");
            String tablePath = "/tmp/test_basic_functionality";
            
            TableStore tableStore = new TableStore(tablePath, primaryKeyFields, partitionFields, 2, schema);
            
            // Write some test data
            List<Map<String, Object>> testData = new ArrayList<>();
            Map<String, Object> record1 = new HashMap<>();
            record1.put("id", "1");
            record1.put("name", "Alice");
            record1.put("age", 25);
            testData.add(record1);
            
            Map<String, Object> record2 = new HashMap<>();
            record2.put("id", "2");
            record2.put("name", "Bob");
            record2.put("age", 30);
            testData.add(record2);
            
            tableStore.write(testData);
            
            // Read the data back
            List<Map<String, Object>> readData = tableStore.read();
            
            if (readData.size() == 2) {
                System.out.println("  Basic read/write test passed");
                return true;
            } else {
                System.out.println("  Basic read/write test failed - expected 2 records, got " + readData.size());
                return false;
            }
            
        } catch (Exception e) {
            System.out.println("  Basic functionality test failed with exception: " + e.getMessage());
            return false;
        }
    }
    
    private static boolean testDeduplication() {
        try {
            System.out.println("Testing deduplication functionality...");
            
            MessageType schema = Types.buildMessage()
                    .addField(Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("id"))
                    .addField(Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("name"))
                    .addField(Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32).named("age"))
                    .named("test_dedup");
            
            List<String> primaryKeyFields = Arrays.asList("id");
            List<String> partitionFields = new ArrayList<>();
            String tablePath = "/tmp/test_deduplication";
            
            TableStore tableStore = new TableStore(tablePath, primaryKeyFields, partitionFields, 2, schema);
            
            // Write test data with duplicates
            List<Map<String, Object>> testData = new ArrayList<>();
            
            // Add first version of record
            Map<String, Object> record1 = new HashMap<>();
            record1.put("id", "1");
            record1.put("name", "Alice");
            record1.put("age", 25);
            testData.add(record1);
            
            // Add duplicate with updated info (should overwrite first)
            Map<String, Object> record2 = new HashMap<>();
            record2.put("id", "1"); // Same primary key
            record2.put("name", "Alice Updated"); // Updated name
            record2.put("age", 26); // Updated age
            testData.add(record2);
            
            // Add another unique record
            Map<String, Object> record3 = new HashMap<>();
            record3.put("id", "2");
            record3.put("name", "Bob");
            record3.put("age", 30);
            testData.add(record3);
            
            tableStore.write(testData);
            
            // Read the data back - should have deduplicated records
            List<Map<String, Object>> readData = tableStore.read();
            
            if (readData.size() == 2) {
                // Check that the deduplicated record has the updated values
                boolean foundUpdatedAlice = false;
                for (Map<String, Object> record : readData) {
                    if ("1".equals(record.get("id")) && "Alice Updated".equals(record.get("name"))) {
                        foundUpdatedAlice = true;
                        break;
                    }
                }
                
                if (foundUpdatedAlice) {
                    System.out.println("  Deduplication test passed - found updated record");
                    return true;
                } else {
                    System.out.println("  Deduplication test failed - updated record not found");
                    return false;
                }
            } else {
                System.out.println("  Deduplication test failed - expected 2 records, got " + readData.size());
                for (Map<String, Object> record : readData) {
                    System.out.println("    " + record);
                }
                return false;
            }
            
        } catch (Exception e) {
            System.out.println("  Deduplication test failed with exception: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static boolean testPartitioning() {
        try {
            System.out.println("Testing partitioning functionality...");
            
            MessageType schema = Types.buildMessage()
                    .addField(Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("id"))
                    .addField(Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("name"))
                    .addField(Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("department"))
                    .named("test_partition");
            
            List<String> primaryKeyFields = Arrays.asList("id");
            List<String> partitionFields = Arrays.asList("department");
            String tablePath = "/tmp/test_partitioning";
            
            TableStore tableStore = new TableStore(tablePath, primaryKeyFields, partitionFields, 2, schema);
            
            // Write test data with different partitions
            List<Map<String, Object>> testData = new ArrayList<>();
            
            Map<String, Object> record1 = new HashMap<>();
            record1.put("id", "1");
            record1.put("name", "Alice");
            record1.put("department", "Engineering");
            testData.add(record1);
            
            Map<String, Object> record2 = new HashMap<>();
            record2.put("id", "2");
            record2.put("name", "Bob");
            record2.put("department", "Marketing");
            testData.add(record2);
            
            Map<String, Object> record3 = new HashMap<>();
            record3.put("id", "3");
            record3.put("name", "Charlie");
            record3.put("department", "Engineering");
            testData.add(record3);
            
            tableStore.write(testData);
            
            // Read the data back
            List<Map<String, Object>> readData = tableStore.read();
            
            if (readData.size() == 3) {
                // Check that all records have proper department values
                boolean hasEngineering = false, hasMarketing = false;
                for (Map<String, Object> record : readData) {
                    String dept = (String) record.get("department");
                    if ("Engineering".equals(dept)) hasEngineering = true;
                    if ("Marketing".equals(dept)) hasMarketing = true;
                }
                
                if (hasEngineering && hasMarketing) {
                    System.out.println("  Partitioning test passed - data properly distributed");
                    return true;
                } else {
                    System.out.println("  Partitioning test failed - missing partition data");
                    return false;
                }
            } else {
                System.out.println("  Partitioning test failed - expected 3 records, got " + readData.size());
                return false;
            }
            
        } catch (Exception e) {
            System.out.println("  Partitioning test failed with exception: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static boolean testErrorHandling() {
        try {
            System.out.println("Testing error handling...");
            
            MessageType schema = Types.buildMessage()
                    .addField(Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("id"))
                    .addField(Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("name"))
                    .named("test_error");
            
            List<String> primaryKeyFields = Arrays.asList("id");
            List<String> partitionFields = new ArrayList<>();
            String tablePath = "/tmp/test_error_handling";
            
            TableStore tableStore = new TableStore(tablePath, primaryKeyFields, partitionFields, 2, schema);
            
            // Test with null records - should handle gracefully
            try {
                tableStore.write(null);
                System.out.println("  Null data test passed - handled gracefully");
            } catch (Exception e) {
                System.out.println("  Null data test failed - threw exception: " + e.getMessage());
                return false;
            }
            
            // Test with records without primary key - should throw exception
            List<Map<String, Object>> testData = new ArrayList<>();
            Map<String, Object> record = new HashMap<>();
            record.put("name", "Alice"); // Missing primary key 'id'
            testData.add(record);
            
            try {
                tableStore.write(testData);
                System.out.println("  Missing primary key validation failed - should have thrown exception");
                return false;
            } catch (Exception e) {
                // This is expected - the validation should catch this
                System.out.println("  Missing primary key validation passed - properly caught error");
            }
            
            return true;
            
        } catch (Exception e) {
            System.out.println("  Error handling test failed with unexpected exception: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static boolean testSchemaValidation() {
        try {
            System.out.println("Testing schema validation...");
            
            // Test with proper schema
            MessageType schema = Types.buildMessage()
                    .addField(Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("id"))
                    .addField(Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("name"))
                    .addField(Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32).named("age"))
                    .named("test_schema");
            
            List<String> primaryKeyFields = Arrays.asList("id");
            List<String> partitionFields = Arrays.asList("age"); // Using age as partition field
            String tablePath = "/tmp/test_schema_validation";
            
            TableStore tableStore = new TableStore(tablePath, primaryKeyFields, partitionFields, 2, schema);
            
            // Write data with proper schema
            List<Map<String, Object>> testData = new ArrayList<>();
            Map<String, Object> record = new HashMap<>();
            record.put("id", "1");
            record.put("name", "Alice");
            record.put("age", 25);
            testData.add(record);
            
            tableStore.write(testData);
            List<Map<String, Object>> readData = tableStore.read();
            
            if (readData.size() == 1) {
                Map<String, Object> result = readData.get(0);
                if ("1".equals(result.get("id")) && "Alice".equals(result.get("name")) && 
                    Integer.valueOf(25).equals(result.get("age"))) {
                    System.out.println("  Schema validation test passed - data properly handled");
                    return true;
                } else {
                    System.out.println("  Schema validation test failed - data mismatch");
                    return false;
                }
            } else {
                System.out.println("  Schema validation test failed - wrong record count");
                return false;
            }
            
        } catch (Exception e) {
            System.out.println("  Schema validation test failed with exception: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
}