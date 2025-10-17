package org.example.tablestore;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableResult;

public class StreamingVerificationTest {
    
    public static void main(String[] args) throws Exception {
        System.out.println("=== Streaming Verification Test ===");
        System.out.println("This test will verify streaming operations with proper streaming queries");
        
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
            "  'table-path' = '/tmp/streaming_verification'," +
            "  'primary-keys' = 'id'" +
            ")"
        );
        
        // Create print sink to see the results
        tableEnv.executeSql(
            "CREATE TABLE print_sink (" +
            "  id STRING," +
            "  name STRING," +
            "  age INT" +
            ") WITH (" +
            "  'connector' = 'print'" +
            ")"
        );
        
        System.out.println("\nStep 1: Starting streaming job to continuously read from tablestore...");
        // This is the correct way to do streaming in Flink - continuously insert from source to sink
        TableResult streamingJob = tableEnv.executeSql(
            "INSERT INTO print_sink SELECT * FROM user_table"
        );
        
        Thread.sleep(3000); // Wait for initialization
        
        System.out.println("\nStep 2: Inserting initial record (should appear in streaming output)...");
        tableEnv.executeSql("INSERT INTO user_table VALUES ('user1', 'Alice', 25)").await();
        
        Thread.sleep(2000);
        
        System.out.println("\nStep 3: Updating record (should appear as updated in streaming output)...");
        tableEnv.executeSql("INSERT INTO user_table VALUES ('user1', 'Alice Updated', 26)").await();
        
        Thread.sleep(2000);
        
        System.out.println("\nStep 4: Inserting another record...");
        tableEnv.executeSql("INSERT INTO user_table VALUES ('user2', 'Bob', 30)").await();
        
        Thread.sleep(3000);
        
        System.out.println("\nStep 5: Canceling streaming job");
        streamingJob.getJobClient().ifPresent(client -> {
            try {
                client.cancel().get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        
        System.out.println("\n=== Streaming Verification Test Completed ===");
        System.out.println("Check the output above for streaming data changes!");
    }
}
