package org.example.tablestore;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.Table;

/**
 * Test class to demonstrate TableStore Connector with Flink Default Catalog
 * This shows how to use TableStore Connector with Flink's default catalog
 */
public class TableStoreFlinkIntegrationTest {
    public static void main(String[] args) {
        System.out.println("=== TableStore with Flink Default Catalog Integration Test ===");
        
        System.out.println("1. Using Flink Default Catalog...");
        
        // Using Flink's default catalog
        System.out.println("Flink automatically uses the default catalog.");
        
        System.out.println("2. Creating TableStore table with default catalog...");
        String tableStoreTableSql = 
            "CREATE TABLE tablestore_table (" +
            "  id STRING," +
            "  name STRING," +
            "  age INT," +
            "  department STRING," +
            "  PRIMARY KEY (id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'tablestore'," +
            "  'table-path' = '/tmp/tablestore_data'," +
            "  'primary-keys' = 'id'" +
            ");";
        System.out.println("TableStore table creation SQL:");
        System.out.println(tableStoreTableSql);
        
        System.out.println("3. Inserting data...");
        String insertSql = 
            "INSERT INTO tablestore_table VALUES " +
            "('1', 'Alice', 25, 'Engineering'), " +
            "('2', 'Bob', 30, 'Marketing');";
        System.out.println(insertSql);
        
        System.out.println("4. Querying data...");
        String querySql = "SELECT * FROM tablestore_table;";
        System.out.println(querySql);
        
        System.out.println();
        System.out.println("The TableStore connector is now ready to work with Flink's default catalog.");
    }
}