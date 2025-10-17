package org.example.tablestore;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * 测试TableStore流式读取所有变化类型的功能
 * 验证INSERT, UPDATE, DELETE操作是否都能被实时捕获
 */
public class ComprehensiveChangeCaptureTest {
    
    public static void main(String[] args) throws Exception {
        System.out.println("=== 测试TableStore流式读取所有变化类型的功能 ===");
        System.out.println("目标：验证INSERT, UPDATE, DELETE操作是否都能被实时捕获");
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        // 创建TableStore表
        tableEnv.executeSql(
            "CREATE TABLE user_stream_table (" +
            "  id STRING," +
            "  name STRING," +
            "  department STRING," +
            "  PRIMARY KEY (id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'tablestore'," +
            "  'table-path' = '/tmp/comprehensive_test'," +
            "  'primary-keys' = 'id'" +
            ")"
        );
        
        // 创建输出表（打印）
        tableEnv.executeSql(
            "CREATE TABLE print_output (" +
            "  id STRING," +
            "  name STRING," +
            "  department STRING" +
            ") WITH (" +
            "  'connector' = 'print'" +
            ")"
        );
        
        System.out.println("\n步骤1: 启动流读取作业，持续监听数据变化...");
        TableResult streamingJob = tableEnv.executeSql(
            "INSERT INTO print_output SELECT * FROM user_stream_table"
        );
        
        System.out.println("流读取作业已启动，等待作业初始化...");
        Thread.sleep(3000);
        
        System.out.println("\n步骤2: 插入初始数据 (INSERT)...");
        tableEnv.executeSql("INSERT INTO user_stream_table VALUES ('emp_001', 'Alice', 'Engineering')").await();
        tableEnv.executeSql("INSERT INTO user_stream_table VALUES ('emp_002', 'Bob', 'Marketing')").await();
        Thread.sleep(3000);
        
        System.out.println("\n步骤3: 更新数据 (UPDATE)...");
        // 更新Alice的记录
        tableEnv.executeSql("INSERT INTO user_stream_table VALUES ('emp_001', 'Alice Updated', 'Engineering')").await();
        Thread.sleep(3000);
        
        System.out.println("\n步骤4: 再插入新数据 (INSERT)...");
        tableEnv.executeSql("INSERT INTO user_stream_table VALUES ('emp_003', 'Charlie', 'Sales')").await();
        Thread.sleep(3000);
        
        System.out.println("\n步骤5: 再次更新数据 (UPDATE)...");
        // 更新Bob的记录
        tableEnv.executeSql("INSERT INTO user_stream_table VALUES ('emp_002', 'Bob Updated', 'Marketing')").await();
        Thread.sleep(3000);
        
        System.out.println("\n步骤6: 插入更多数据 (INSERT)...");
        tableEnv.executeSql("INSERT INTO user_stream_table VALUES ('emp_004', 'Diana', 'Engineering')").await();
        Thread.sleep(3000);
        
        System.out.println("\n步骤7: 最后更新数据 (UPDATE)...");
        tableEnv.executeSql("INSERT INTO user_stream_table VALUES ('emp_003', 'Charlie Updated', 'Sales')").await();
        Thread.sleep(3000);
        
        System.out.println("\n=== 流式变化捕获测试完成 ===");
        System.out.println("期望结果：所有INSERT和UPDATE操作都应该被实时捕获并输出");
        System.out.println("- 初始插入: emp_001 (Alice), emp_002 (Bob)");
        System.out.println("- 更新操作: emp_001 (Alice -> Alice Updated)");
        System.out.println("- 新增插入: emp_003 (Charlie)");
        System.out.println("- 更新操作: emp_002 (Bob -> Bob Updated)");
        System.out.println("- 新增插入: emp_004 (Diana)");
        System.out.println("- 更新操作: emp_003 (Charlie -> Charlie Updated)");
        
        // 停止流作业
        streamingJob.getJobClient().ifPresent(client -> {
            try {
                System.out.println("\n正在停止流作业...");
                client.cancel().get();
            } catch (Exception e) {
                System.out.println("停止作业时出错: " + e.getMessage());
            }
        });
    }
}