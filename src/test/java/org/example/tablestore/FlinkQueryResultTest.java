package org.example.tablestore;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.example.tablestore.flink.sql.TableStoreCatalog;
import org.apache.flink.types.Row;

/**
 * A test to diagnose and fix the issue where Flink queries return no results.
 * This focuses on the core problem: data not being properly stored or retrieved.
 */
public class FlinkQueryResultTest {

    public static void main(String[] args) {
        System.out.println("=== Flink Query Result Test ===");
        System.out.println();
        
        String catalogName = "test_catalog";
        String databaseName = "test_db";
        String tableName = "test_table";
        String warehousePath = "/tmp/flink_query_test";
        
        try {
            // Create Flink Table Environment in batch mode
            EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
            TableEnvironment tableEnv = TableEnvironment.create(settings);
            
            System.out.println("1. Creating TableStore catalog...");
            TableStoreCatalog catalog = new TableStoreCatalog(catalogName, warehousePath);
            tableEnv.registerCatalog(catalogName, catalog);
            tableEnv.useCatalog(catalogName);
            System.out.println("✓ Catalog registered: " + catalogName);
            
            // Create database
            tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + databaseName);
            tableEnv.useDatabase(databaseName);
            System.out.println("✓ Database created: " + databaseName);
            
            System.out.println();
            System.out.println("2. Creating table with TableStore connector...");
            // Create table that uses the tablestore connector
            String createTableSQL = 
                "CREATE TABLE " + tableName + " (" +
                "  id BIGINT," +
                "  name STRING," +
                "  age INT," +
                "  PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'tablestore'," +
                "  'table-path' = '" + warehousePath + "/data/" + tableName + "'," +
                "  'primary-keys' = 'id'," +
                "  'num-buckets' = '2'" +
                ")";
            
            tableEnv.executeSql(createTableSQL);
            System.out.println("✓ Table created with TableStore connector: " + tableName);
            
            System.out.println();
            System.out.println("3. Inserting data into the table...");
            // Insert some test data
            String insertSQL = 
                "INSERT INTO " + tableName + " VALUES " +
                "  (1, 'Alice', 25)," +
                "  (2, 'Bob', 30)," +
                "  (3, 'Charlie', 35)";
            
            // Execute the insert operation
            tableEnv.executeSql(insertSQL);
            System.out.println("✓ Data inserted successfully");
            
            System.out.println();
            System.out.println("4. Querying the data - checking if any results are returned...");
            
            // Query 1: Select all data
            System.out.println("Query 1: SELECT * FROM " + tableName);
            org.apache.flink.util.CloseableIterator<Row> resultIterator = 
                tableEnv.executeSql("SELECT * FROM " + tableName).collect();
            
            int count = 0;
            while (resultIterator.hasNext()) {
                Row row = resultIterator.next();
                System.out.println("  Row " + (++count) + ": " + row);
            }
            
            if (count == 0) {
                System.out.println("  ❌ ISSUE CONFIRMED: No results returned from query!");
            } else {
                System.out.println("  ✓ SUCCESS: Found " + count + " rows");
            }
            
            System.out.println();
            System.out.println("5. Querying with filter...");
            // Query 2: Filter query
            System.out.println("Query 2: SELECT * FROM " + tableName + " WHERE age > 25");
            org.apache.flink.util.CloseableIterator<Row> filterIterator = 
                tableEnv.executeSql("SELECT * FROM " + tableName + " WHERE age > 25").collect();
            
            int filterCount = 0;
            while (filterIterator.hasNext()) {
                Row row = filterIterator.next();
                System.out.println("  Filtered Row " + (++filterCount) + ": " + row);
            }
            
            if (filterCount == 0) {
                System.out.println("  ❌ ISSUE CONFIRMED: No results returned from filtered query!");
            } else {
                System.out.println("  ✓ SUCCESS: Found " + filterCount + " filtered rows");
            }
            
            System.out.println();
            System.out.println("6. Diagnostic checks:");
            
            // Show available tables
            String[] tables = tableEnv.listTables();
            System.out.println("   Tables in database: " + java.util.Arrays.toString(tables));
            
            // Check if data directory exists
            java.nio.file.Path dataPath = java.nio.file.Paths.get(warehousePath, "data", tableName);
            System.out.println("   ✓ Data directory exists: " + java.nio.file.Files.exists(dataPath));
            
            if (java.nio.file.Files.exists(dataPath)) {
                System.out.println("   Data files in directory:");
                java.nio.file.Files.walk(dataPath)
                    .filter(java.nio.file.Files::isRegularFile)
                    .forEach(file -> System.out.println("     - " + file.getFileName()));
            }
            
            System.out.println();
            System.out.println("=== Test Summary ===");
            if (count == 0) {
                System.out.println("❌ PROBLEM IDENTIFIED:");
                System.out.println("   - Data was inserted but queries return no results");
                System.out.println("   - This indicates an issue in the read path of TableStore");
            } else {
                System.out.println("✓ SUCCESS: Queries are returning data as expected");
            }
            
            // Cleanup
            catalog.close();
            
        } catch (Exception e) {
            System.err.println("Error during test execution: " + e.getMessage());
            e.printStackTrace();
        }
    }
}