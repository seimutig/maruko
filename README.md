# TableStore: Maruko-Style Table Format Implementation with Flink Connector

A complete implementation of a lakehouse table format inspired by Maruko, featuring merge-on-read functionality with primary key deduplication and **full Flink streaming integration**.

**Note**: For comprehensive documentation on the productional Flink connector implementation, see [COMPREHENSIVE_DOCUMENTATION.md](COMPREHENSIVE_DOCUMENTATION.md).

## 🚀 NEW: LSM-Tree Architecture Added!

The latest version includes a full LSM-tree (Log-Structured Merge Tree) implementation that brings the storage architecture closer to production-grade systems like Maruko.

## Architecture: Flink + TableStore Connector

This implementation uses a **segregated architecture** where:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Flink SQL     │    │  Flink Table API │    │ Flink DataStream│
│   Interface     │    │    Interface     │    │   Interface     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                      │
         └───────────────────────┼──────────────────────┘
                                 │
                   ┌─────────────▼─────────────┐
                   │   TableStore Factory     │
                   │ (Source/Sink Creation)    │
                   └─────────────┬─────────────┘
                                 │
                   ┌─────────────▼─────────────┐
                   │    TableStore Core        │
                   │ (Merge-on-Read Engine)    │
                   └─────┬─────────────┬───────┘
                         │             │
          ┌──────────────▼──┐     ┌────▼──────────────────┐
          │   LSM-Tree      │     │   Snapshot Manager    │
          │ (Storage Engine)│     │ (Consistency Control) │
          └─────────────────┘     └───────────────────────┘
                   │
          ┌────────▼────────┐
          │   File Store    │
          │ (Data Storage)  │
          └─────────────────┘
```

## Features

### Core Storage Features
- **Merge-on-Read with Deduplication**: Keep latest records based on primary key and sequence number
- **Partitioning & Bucketing**: Hierarchical data organization for efficient querying and parallel processing
- **Snapshot Management**: Point-in-time consistency for ACID transactions
- **Manifest System**: Track data files and metadata for efficient operations
- **Compaction Strategies**: Merge small files and remove duplicates
- **LSM-Tree Architecture**: Production-grade storage hierarchy

### Flink Integration Features
- **Full Table API Support**: CREATE TABLE, INSERT, SELECT with SQL for streaming operations
- **DataStream API Integration**: SourceFunction and SinkFunction for programmatic streaming access
- **Flink Catalog Integration**: Database/table metadata management via Flink's default catalog
- **RowData Format**: Native Flink integration with zero-copy operations
- **Streaming-Only Mode**: Continuous unbounded data processing (**batch mode not supported**)

## LSM-Tree Architecture Details

### Level Structure
```
Level 0 (L0): Newest files, small size, frequent writes
├── Max 4 files before compaction triggered
├── Target file size: 64MB
└── Most fragmented data

Level 1 (L1): Compacted from L0, medium size
├── Target file size: 128MB
└── Less fragmentation than L0

Level 2 (L2): Larger files, less frequent access
├── Target file size: 256MB
└── Infrequent compaction

Level 3 (L3): Base level, largest files
├── Target file size: 512MB
└── Rare compaction, base data layer
```

### Compaction Strategy
1. **Size-Tiered Compaction**: When level N exceeds size ratio, compact to level N+1
2. **Automatic Triggering**: Based on file count and size thresholds
3. **Recursive Cascading**: Compaction at one level can trigger compaction at next level
4. **Deduplication During Compaction**: Old versions removed during merge process

## Project Structure

```
tablestore/
├── core/                 # Core TableStore engine
│   ├── TableStore.java        # Main entry point
│   ├── MergeOnReadManager.java # Deduplication logic
│   ├── BucketAssigner.java     # Partition/bucket logic
│   └── CompactionManager.java  # File compaction
├── format/               # Metadata and file format
│   ├── Snapshot.java          # Snapshot management
│   ├── ManifestEntry.java     # File tracking
│   └── ManifestManager.java    # Manifest operations
├── io/                   # File I/O operations
│   └── FileStore.java         # File system interface
├── lsm/                  # NEW: LSM-tree implementation
│   ├── LSMTree.java          # Core LSM-tree logic
│   ├── LSMLevel.java         # Level management
│   ├── LSMFile.java          # File wrapper with metadata
│   ├── LSMTreeConfig.java    # Configuration
│   └── LSMTreeStats.java     # Statistics
├── flink/                # Flink integration layer
│   ├── sql/                   # Table API integration
│   │   ├── TableStoreTableSource.java  # Table source
│   │   ├── TableStoreTableSink.java    # Table sink
│   │   └── TableStoreDynamicTableFactory.java # Factory for connector
│   ├── TableStoreInputFormat.java      # DataStream source
│   └── TableStoreOutputFormat.java     # DataStream sink
└── examples/             # Usage examples and demos
```

## Prerequisites

- Java 8 or higher
- Maven 3.6+
- Apache Flink 1.17.1 (included as dependency)
- Apache Hadoop 3.3.4 (included as dependency)


## Build Instructions

```bash
# Clone the repository
git clone <repository-url>
cd maruko-project

# Compile and package
mvn clean package

# The JAR will be created in target/ directory
ls -la target/maruko-tablestore-1.0-SNAPSHOT.jar
```

## Running Examples

### 1. Run the Comprehensive Demo

```bash
# Run the main demo showing all features
mvn exec:java -Dexec.mainClass="org.example.tablestore.TableStoreDemo"

# Run validation tests
mvn exec:java -Dexec.mainClass="org.example.tablestore.TableStoreValidationTest"

# Run Flink integration test
mvn exec:java -Dexec.mainClass="org.example.tablestore.flink.sql.TableStoreFlinkIntegrationTest"

# NEW: Run LSM-tree demo
mvn exec:java -Dexec.mainClass="org.example.tablestore.lsm.LSMTreeDemo"
```

### 3. Flink SQL Usage Example

```sql
-- Using TableStore connector with Flink's default catalog for streaming operations

-- Create a table with TableStore connector (streaming-only)
CREATE TABLE users (
  id STRING,
  name STRING,
  age INT,
  salary DOUBLE,
  department STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'tablestore',
  'table-path' = '/tmp/users_table',  -- Physical data location
  'primary-keys' = 'id',
  'partition-keys' = 'department',
  'num-buckets' = '4'
);
```

### 3. Programmatic Usage
```java
// Direct TableStore usage with LSM-tree
// (Bypassing Flink when using directly)

// With Flink Integration:
// - Table metadata (schema, locations) stored in Flink's default catalog
// - Actual data stored and managed by TableStore with LSM-tree
// - Flink uses TableStore connector for read/write operations
```

## Key Design Decisions

### 1. **Flink Default Catalog Integration**
Using Flink's default catalog for metadata management, allowing:
- Standardized metadata management
- Built-in compatibility with Flink ecosystem
- Simple configuration and deployment

### 2. **LSM-Tree Architecture**
Files are organized hierarchically with multiple levels, enabling:
- Better write performance through leveled compaction
- More efficient read operations with intelligent file selection
- Reduced I/O through file-level metadata

### 3. **Merge-on-Read Architecture**
Still applies deduplication during reads for consistency while optimizing writes.

### 4. **Primary Key Based Deduplication**
Records are deduplicated based on primary key fields with sequence numbers.

### 5. **Hierarchical Data Organization**
Data is organized hierarchically with partitioning, bucketing, and LSM-tree levels.

### 6. **Snapshot-Based Consistency**
Point-in-time snapshots ensure ACID transaction semantics.

## Error Handling & Validation

The implementation includes comprehensive error handling:

- **Input Validation**: Checks for required fields, proper schema compliance
- **Null Safety**: Graceful handling of null values throughout the pipeline
- **Resource Management**: Proper cleanup of resources and file handles
- **Concurrency Control**: Thread-safe operations with appropriate locking
- **Graceful Degradation**: Failures handled without system crashes

## Performance Considerations

### Optimizations Implemented

1. **LSM-Tree Structure**: Hierarchical file organization for efficient operations
2. **Automated Compaction**: Size-tiered compaction reduces fragmentation
3. **Efficient Data Layout**: Partition pruning reduces I/O operations
4. **Smart Deduplication**: Merge-on-read avoids write amplification
5. **Streaming Processing**: Continuous operations improve throughput

### Scalability Features

- **Horizontal Scaling**: Bucketing enables parallel processing
- **Partition Pruning**: Selective reads improve query performance
- **Parallel Processing**: File-level parallelism for I/O operations
- **Level-Based Optimization**: Different strategies per LSM level

## Testing

The project includes comprehensive testing covering:

- ✅ Basic read/write operations
- ✅ Deduplication functionality
- ✅ Partitioning and bucketing
- ✅ Error handling and edge cases
- ✅ Schema validation
- ✅ Flink integration scenarios
- ✅ LSM-tree compaction and performance

All tests pass successfully, demonstrating the robustness of the implementation.

## Future Enhancements

Potential areas for future improvement:

1. **Advanced Compaction Strategies**: Size-tiered, leveled compaction tuning
2. **Schema Evolution**: More sophisticated schema change handling
3. **Index Support**: Secondary indexes for non-primary key lookups
4. **Advanced Query Optimization**: Predicate pushdown, column pruning
5. **Streaming Materialized Views**: Real-time aggregations and rollups
6. **Cloud Storage Optimization**: S3/GCS specific optimizations
7. **Bloom Filters**: File-level existence checking for faster queries
8. **Zone Maps**: Range-based file filtering

## Conclusion

This implementation successfully demonstrates all the core concepts of a modern lakehouse table format with Flink connector integration. The system provides:

- **Production-grade reliability** with comprehensive error handling
- **Full Flink integration** supporting both Table and DataStream APIs
- **Efficient data processing** through smart partitioning and deduplication
- **Scalable architecture** designed for large-scale deployments
- **LSM-tree storage** bringing it closer to systems like Maruko
- **Flink catalog compatibility** for standardized metadata management
- **Extensible design** allowing for future feature additions

## Controlling Debug Output\n\nThe project now uses a dual-mode logging system that supports both standalone and Flink cluster environments:\n\n### Dual-Mode Logging System\n\nMaruko TableStore supports two logging modes:\n1. **Standalone Mode**: Uses SimpleLogger for local development and testing\n2. **Flink Cluster Mode**: Integrates with Flink's Log4j configuration system\n\nThis provides flexibility to control log output in both local development and production cluster environments.\n\n### Standalone Mode (Local Development)\n\nIn local development, the project uses SLF4J SimpleLogger with configuration from `simplelogger.properties`:\n\n#### Via Configuration File\nCreate or modify `src/main/resources/simplelogger.properties`:\n```properties\n# Set root log level\norg.slf4j.simpleLogger.defaultLogLevel=INFO\n\n# Set specific package log level\norg.slf4j.simpleLogger.log.org.example.tablestore=DEBUG\n\n# Third-party libraries\norg.slf4j.simpleLogger.log.org.apache.flink=WARN\norg.slf4j.simpleLogger.log.org.apache.hadoop=WARN\n```\n\n#### Via System Properties\n```bash\n# Set root log level to WARN to reduce output\njava -Dorg.slf4j.simpleLogger.defaultLogLevel=WARN -jar your-application.jar\n\n# Set specific package log level to DEBUG for detailed information\njava -Dorg.slf4j.simpleLogger.log.org.example.tablestore=DEBUG -jar your-application.jar\n\n# Combine settings for fine-grained control\njava -Dorg.slf4j.simpleLogger.defaultLogLevel=INFO \\\n     -Dorg.slf4j.simpleLogger.log.org.example.tablestore=DEBUG \\\n     -Dorg.slf4j.simpleLogger.log.org.apache.flink=WARN \\\n     -jar your-application.jar\n```\n\n### Flink Cluster Mode\n\nIn Flink cluster environments, Maruko automatically integrates with Flink's Log4j configuration system:\n\n#### Via Flink's log4j.properties\nModify Flink's `conf/log4j.properties`:\n```properties\n# Set Maruko log level\nlogger.maruko.name = org.example.tablestore\nlogger.maruko.level = INFO\n\n# Third-party libraries\nlogger.flink.name = org.apache.flink\nlogger.flink.level = WARN\n```\n\n#### Via Flink Job Submission\n```bash\n# Set log levels when submitting Flink jobs\n./bin/flink run -Denv.java.opts=\"-Dorg.slf4j.simpleLogger.log.org.example.tablestore=DEBUG\" your-job.jar\n```\n\n### In Code\n\nYou can control logging programmatically using the MarukoLogger utility:\n\n```java\nimport org.example.tablestore.MarukoLogger;\nimport org.slf4j.Logger;\n\n// Use the unified MarukoLogger interface\nMarukoLogger.debug(\"This is a debug message\");\nMarukoLogger.info(\"This is an info message\");\nMarukoLogger.warn(\"This is a warning message\");\nMarukoLogger.error(\"This is an error message\");\n\n// Or get a standard SLF4J logger\nLogger logger = MarukoLogger.getLogger(MyClass.class);\nlogger.debug(\"This is a debug message using standard SLF4J\");\n```\n\n### Log Levels\n\nSLF4J supports the following log levels (from most to least verbose):\n1. **TRACE** - Very detailed diagnostic information\n2. **DEBUG** - Detailed information for diagnosing problems\n3. **INFO** - General information about application progress\n4. **WARN** - Potentially harmful situations\n5. **ERROR** - Error events that might still allow the application to continue\n\nFor more detailed information about logging configuration, see [LOGGING_CONFIGURATION.md](LOGGING_CONFIGURATION.md).\n