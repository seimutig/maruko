package org.example.tablestore;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.example.tablestore.flink.sql.TableStoreCatalog;
import org.apache.flink.types.Row;

/**
 * Final verification test to confirm that Flink queries now return results
 */
public class FinalVerificationTest {

    public static void main(String[] args) {
        System.out.println("=== FINAL VERIFICATION: Flink Query Results Test ===");
        System.out.println();
        
        String catalogName = "final_test_catalog";
        String databaseName = "final_test_db";
        String tableName = "final_test_table";
        String warehousePath = "/tmp/final_verification_test";
        
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
            System.out.println("4. QUERYING THE DATA - checking if results are now returned...");
            
            // Query 1: Select all data
            System.out.println("Query 1: SELECT * FROM " + tableName);
            org.apache.flink.util.CloseableIterator<Row> resultIterator = 
                tableEnv.executeSql("SELECT * FROM " + tableName).collect();
            
            int count = 0;
            System.out.println("Results:");
            while (resultIterator.hasNext()) {
                Row row = resultIterator.next();
                System.out.println("  Row " + (++count) + ": " + row);
            }
            
            if (count == 0) {
                System.out.println("❌ PROBLEM STILL EXISTS: No results returned from query!");
            } else {
                System.out.println("✅ SUCCESS: Found " + count + " rows - THE FIX IS WORKING!");
            }
            
            System.out.println();
            System.out.println("5. Querying with filter...");
            // Query 2: Filter query
            System.out.println("Query 2: SELECT * FROM " + tableName + " WHERE age > 25");
            org.apache.flink.util.CloseableIterator<Row> filterIterator = 
                tableEnv.executeSql("SELECT * FROM " + tableName + " WHERE age > 25").collect();
            
            int filterCount = 0;
            System.out.println("Filtered Results:");
            while (filterIterator.hasNext()) {
                Row row = filterIterator.next();
                System.out.println("  Filtered Row " + (++filterCount) + ": " + row);
            }
            
            if (filterCount == 0) {
                System.out.println("❌ Filter query returned no results");
            } else {
                System.out.println("✅ Filter query successful: Found " + filterCount + " filtered rows");
            }
            
            System.out.println();
            System.out.println("=== FINAL VERIFICATION SUMMARY ===");
            if (count > 0) {
                System.out.println("🎉 SUCCESS: Flink queries are now returning data as expected!");
                System.out.println("   - Query 1 returned: " + count + " rows");
                System.out.println("   - Filter query returned: " + filterCount + " rows");
                System.out.println("   - Issue has been FIXED! 🎉");
            } else {
                System.out.println("❌ ISSUE STILL EXISTS: Queries return no results");
            }
            
            // Cleanup
            catalog.close();
            
        } catch (Exception e) {
            System.err.println("Error during final verification test: " + e.getMessage());
            e.printStackTrace();
        }
    }
}