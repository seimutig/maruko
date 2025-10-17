import org.example.tablestore.core.TableStore;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.util.*;

public class ManualCheckpointTest {
    public static void main(String[] args) {
        try {
            MessageType schema = Types.buildMessage()
                    .addField(Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("id"))
                    .addField(Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY).named("name"))
                    .addField(Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("timestamp"))
                    .named("test_record");
            
            List<String> primaryKeyFields = Arrays.asList("id");
            List<String> partitionFields = Arrays.asList("date");
            
            String tablePath = "/tmp/test_checkpoint_tablestore";
            TableStore tableStore = new TableStore(tablePath, primaryKeyFields, partitionFields, 4, schema);
            
            System.out.println("Testing checkpoint-based TableStore...");
            
            List<Map<String, Object>> testData = new ArrayList<>();
            Map<String, Object> record = new HashMap<>();
            record.put("id", "user_1");
            record.put("name", "Alice");
            record.put("timestamp", 1000L);
            record.put("date", "2023-01-01");
            testData.add(record);
            
            System.out.println("Writing test data to tablestore...");
            tableStore.write(testData);
            
            System.out.println("Reading data back (should be empty without checkpoint):...");
            List<Map<String, Object>> readData = tableStore.read();
            System.out.println("Read " + readData.size() + " records (should be 0 before checkpoint): " + readData);
            
            System.out.println("Creating checkpoint...");
            tableStore.checkpoint(); // This should create a snapshot
            
            System.out.println("Reading data back after checkpoint:...");
            readData = tableStore.read();
            System.out.println("Read " + readData.size() + " records after checkpoint: " + readData);
            
            System.out.println("Manual checkpoint test completed successfully!");
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
