package org.example.tablestore;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableResult;

public class TwoTableStreamingTest {
    
    public static void main(String[] args) throws Exception {
        System.out.println("=== Two-Table Streaming Test ===");
        System.out.println("Testing proper streaming: write to source table -> stream read to sink table");
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        // Create source table (where we'll write data) 
        tableEnv.executeSql(
            "CREATE TABLE source_table (" +
            "  id STRING," +
            "  name STRING," +
            "  age INT," +
            "  PRIMARY KEY (id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'tablestore'," +
            "  'table-path' = '/tmp/source_table'," +
            "  'primary-keys' = 'id'" +
            ")"
        );
        
        // Create sink table (where streaming data will be read to)
        tableEnv.executeSql(
            "CREATE TABLE sink_table (" +
            "  id STRING," +
            "  name STRING," +
            "  age INT" +
            ") WITH (" +
            "  'connector' = 'print'" +  // Using print connector to see the streaming data
            ")"
        );
        
        System.out.println("Step 1: Setting up streaming job to read from source_table and write to sink_table...");
        
        // Start the streaming job: continuously read from source and print to sink
        TableResult streamingJob = tableEnv.executeSql(
            "INSERT INTO sink_table SELECT * FROM source_table"
        );
        
        Thread.sleep(3000); // Wait for initialization
        
        System.out.println("\nStep 2: Writing data to source_table (should appear in streaming output)...");
        
        // Write to source table - this should trigger changelog operations that flow to sink
        tableEnv.executeSql("INSERT INTO source_table VALUES ('user1', 'Alice', 25)").await();
        System.out.println("  Inserted user1 (Alice, 25) into source_table");
        
        Thread.sleep(2000);
        
        // Update operation
        tableEnv.executeSql("INSERT INTO source_table VALUES ('user1', 'Alice Updated', 26)").await();
        System.out.println("  Updated user1 (Alice -> Alice Updated) in source_table");
        
        Thread.sleep(2000);
        
        // Another insert
        tableEnv.executeSql("INSERT INTO source_table VALUES ('user2', 'Bob', 30)").await();
        System.out.println("  Inserted user2 (Bob, 30) into source_table");
        
        Thread.sleep(3000);
        
        System.out.println("\nStep 3: Cancelling streaming job");
        streamingJob.getJobClient().ifPresent(client -> {
            try {
                client.cancel().get();
                System.out.println("Streaming job cancelled successfully");
            } catch (Exception e) {
                System.out.println("Error cancelling job: " + e.getMessage());
                e.printStackTrace();
            }
        });
        
        System.out.println("\n=== Two-Table Streaming Test Completed ===");
        System.out.println("Check the output above for streaming data changes from source to sink!");
        System.out.println("Expected output:");
        System.out.println("- Initial state (if any)");
        System.out.println("- INSERT: user1 (Alice, 25)"); 
        System.out.println("- UPDATE: user1 (Alice -> Alice Updated)");
        System.out.println("- INSERT: user2 (Bob, 30)");
    }
}
