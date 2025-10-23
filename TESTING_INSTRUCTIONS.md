# Flink Connector Test Instructions

This project contains three separate test classes to properly test the complete Flink connector data flow:

## Three-Component Testing Approach

The test scenario involves 3 tables/components:
1. **TestDataGenerationJob.java**: Generates data using datagen connector and populates the tablestore source table
2. **TestDataStreamJob.java**: Streams data from tablestore source to tablestore sink using "INSERT INTO user_sink SELECT * FROM user_source"
3. **TestQuerySinkJob.java**: Queries the sink to verify data (one-time job)

## TableStore Connector Configuration

The TableStore connector requires the following key parameters:

- `table-path`: Path to the TableStore data storage location
- `primary-keys`: Comma-separated list of primary key fields for deduplication
- `is-bounded`: Whether the source is bounded (batch) or unbounded (streaming) (default: false)

## How to Run

### Prerequisites
- Make sure you have Java 8+ and Maven installed
- Set up your IDE with Java support (IntelliJ IDEA, Eclipse, etc.)

### Running in IDE (Recommended due to Maven exec plugin classpath issues)

1. **First, compile the project:**
   ```bash
   cd ~/maruko_project
   mvn clean compile
   ```

2. **Run the data generation job (TestDataGenerationJob):**
   - Open `TestDataGenerationJob.java` in your IDE
   - Right-click and select "Run 'TestDataGenerationJob.main()'"
   - This job uses `datagen` to generate continuous data and inserts it into the tablestore source table
   - The source table is configured with:
     - `table-path`: '/tmp/user_source_tablestore'
     - `primary-keys`: 'id'
   - Let it run for a few minutes to populate the source table with data

3. **In a separate terminal/IDE window, run the streaming job (TestDataStreamJob):**
   - Open `TestDataStreamJob.java` in your IDE
   - Right-click and select "Run 'TestDataStreamJob.main()'"
   - This job streams data from tablestore source to tablestore sink using "INSERT INTO user_sink SELECT * FROM user_source"
   - Both source and sink tables use the tablestore connector with proper configuration

4. **Finally, run the verification job (TestQuerySinkJob) in another window:**
   - Open `TestQuerySinkJob.java` in your IDE
   - Right-click and select "Run 'TestQuerySinkJob.main()'"
   - This will query the tablestore sink and verify that data has been written

### Important Notes
- Due to Maven exec plugin classpath issues when running Flink jobs, always run tests from within your IDE
- Both the data generation job (TestDataGenerationJob) and streaming job (TestDataStreamJob) will run indefinitely - you can stop them manually when testing is complete
- The query job (TestQuerySinkJob) will complete once it has verified the data
- Make sure the `table-path` directories exist and are writable by the application
- The `is-bounded` parameter is used to specify whether the table should operate in batch (true) or streaming (false) mode
- The datagen connector provides continuous random data simulation

### Customizing the Tests
- Modify the connector configurations in all files according to your specific requirements
- Adjust the table schemas to match your data structure
- Update `table-path` and `primary-keys` as needed for your environment
- The datagen connector provides continuous random data simulation