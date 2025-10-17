package org.example.tablestore;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * 专门测试TableStore流式读取功能的测试
 * 验证SELECT查询启动后，新插入的数据是否能被实时捕获
 */
public class StreamingReadTest {
    
    public static void main(String[] args) throws Exception {
        System.out.println("=== 专门测试TableStore流式读取功能 ===");
        System.out.println("目标：验证SELECT查询启动后，新插入的数据是否能被实时捕获");
        
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
            "  'table-path' = '/tmp/streaming_read_test'," +
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
        // 启动一个持续的流读取作业 - 这是关键！
        // 一旦启动这个作业，它应该持续运行并监听新数据
        TableResult streamingJob = tableEnv.executeSql(
            "INSERT INTO print_output SELECT * FROM user_stream_table"
        );
        
        System.out.println("流读取作业已启动，等待作业初始化...");
        // 等待作业初始化
        Thread.sleep(3000);
        
        System.out.println("\n步骤2: 在流读取作业运行中插入数据，看是否能被实时捕获...");
        // 在流读取作业运行期间插入数据
        tableEnv.executeSql("INSERT INTO user_stream_table VALUES ('emp_001', 'Alice', 'Engineering')").await();
        
        Thread.sleep(3000); // 等待处理
        
        System.out.println("\n步骤3: 再插入一条数据...");
        tableEnv.executeSql("INSERT INTO user_stream_table VALUES ('emp_002', 'Bob', 'Marketing')").await();
        
        Thread.sleep(3000); // 等待处理
        
        System.out.println("\n步骤4: 再插入一条数据...");
        tableEnv.executeSql("INSERT INTO user_stream_table VALUES ('emp_003', 'Charlie', 'Sales')").await();
        
        Thread.sleep(5000); // 等待处理更长时间
        
        System.out.println("\n步骤5: 停止流作业");
        streamingJob.getJobClient().ifPresent(client -> {
            try {
                System.out.println("正在停止流作业...");
                client.cancel().get(); // 等待作业取消完成
            } catch (Exception e) {
                System.out.println("停止作业时出错: " + e.getMessage());
            }
        });
        
        System.out.println("\n=== 流式读取测试完成 ===");
        System.out.println("如果在输出中看到插入的数据被打印出来，说明流式读取功能正常工作！");
        System.out.println("期望结果：插入的每条数据('emp_001', 'emp_002', 'emp_003')都应该被实时捕获并输出");
    }
}