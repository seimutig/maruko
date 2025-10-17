package org.example.tablestore;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.CompletableFuture;

public class ChangelogVerificationTest {
    
    public static void main(String[] args) throws Exception {
        System.out.println("=== Testing Flink Stream Read Changelog Operations ===");
        System.out.println("This test will verify if INSERT/UPDATE/DELETE operations are captured correctly");
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        // Create TableStore table
        tableEnv.executeSql(
            "CREATE TABLE user_table (" +
            "  id STRING," +
            "  name STRING," +
            "  age INT," +
            "  PRIMARY KEY (id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'tablestore'," +
            "  'table-path' = '/tmp/changelog_test'," +
            "  'primary-keys' = 'id'" +
            ")"
        );
        
        // Create print output table to see the changelog
        tableEnv.executeSql(
            "CREATE TABLE print_output (" +
            "  id STRING," +
            "  name STRING," +
            "  age INT" +
            ") WITH (" +
            "  'connector' = 'print'" +
            ")"
        );
        
        System.out.println("Step 1: Starting streaming read to capture all operations...");
        TableResult streamingJob = tableEnv.executeSql(
            "INSERT INTO print_output SELECT * FROM user_table"
        );
        
        Thread.sleep(3000); // Wait for initialization
        
        System.out.println("\nStep 2: Inserting initial records (INSERT operations)");
        tableEnv.executeSql("INSERT INTO user_table VALUES ('user1', 'Alice', 25)").await();
        tableEnv.executeSql("INSERT INTO user_table VALUES ('user2', 'Bob', 30)").await();
        
        Thread.sleep(2000);
        
        System.out.println("\nStep 3: Updating a record (UPDATE operation)");
        tableEnv.executeSql("INSERT INTO user_table VALUES ('user1', 'Alice Updated', 26)").await(); // This should be an UPDATE
        
        Thread.sleep(2000);
        
        System.out.println("\nStep 4: Inserting another new record (INSERT operation)");
        tableEnv.executeSql("INSERT INTO user_table VALUES ('user3', 'Charlie', 35)").await();
        
        Thread.sleep(3000);
        
        System.out.println("\nStep 5: Stopping the streaming job");
        streamingJob.getJobClient().ifPresent(client -> {
            try {
                client.cancel().get();
            } catch (Exception e) {
                System.out.println("Error stopping job: " + e.getMessage());
            }
        });
        
        System.out.println("\n=== Changelog Test Completed ===");
        System.out.println("Expected output in print:");
        System.out.println("- Initial state from snapshot (if any)"); 
        System.out.println("- INSERT: user1, user2");
        System.out.println("- UPDATE: user1 (name and age changed)"); 
        System.out.println("- INSERT: user3");
    }
}
