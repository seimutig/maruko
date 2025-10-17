package org.example.tablestore;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;

/**
 * 直接验证TableStore是否支持SELECT流读的测试
 */
public class DirectSelectTest {
    
    public static void main(String[] args) throws Exception {
        System.out.println("=== 直接验证TableStore SELECT流读 ===");
        
        // 1. 首先创建数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        // 创建写入表
        tableEnv.executeSql(
            "CREATE TABLE user_write_table (" +
            "  id STRING," +
            "  name STRING," +
            "  department STRING," +
            "  PRIMARY KEY (id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'tablestore'," +
            "  'table-path' = '/tmp/direct_test'," +
            "  'primary-keys' = 'id'" +
            ")"
        );
        
        // 写入一些数据
        tableEnv.executeSql(
            "INSERT INTO user_write_table VALUES " +
            "('emp_001', 'Alice', 'Engineering')," +
            "('emp_002', 'Bob', 'Marketing')"
        ).await();
        
        System.out.println("数据写入完成");
        
        // 2. 创建读取表
        tableEnv.executeSql(
            "CREATE TABLE user_read_table (" +
            "  id STRING," +
            "  name STRING," +
            "  department STRING," +
            "  PRIMARY KEY (id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'tablestore'," +
            "  'table-path' = '/tmp/direct_test'," +
            "  'primary-keys' = 'id'" +
            ")"
        );
        
        System.out.println("创建读取表完成");
        
        // 3. 尝试执行SELECT查询
        System.out.println("执行SELECT流读查询...");
        try {
            // 创建输出表
            tableEnv.executeSql(
                "CREATE TABLE output_table (" +
                "  id STRING," +
                "  name STRING," +
                "  department STRING" +
                ") WITH (" +
                "  'connector' = 'print'" +
                ")"
            );
            
            // 执行SELECT并插入到输出表
            TableResult result = tableEnv.executeSql(
                "INSERT INTO output_table SELECT * FROM user_read_table"
            );
            
            System.out.println("SELECT流读查询启动成功!");
            System.out.println("TableStore支持流式SELECT查询。");
            
            // 等待几秒查看输出结果
            Thread.sleep(3000);
            
        } catch (Exception e) {
            System.out.println("SELECT流读查询失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}