# Maruko TableStore 日志配置指南

## 概述

Maruko TableStore 支持双模式日志配置：
1. **独立模式**：使用 SimpleLogger，在本地开发和测试时使用
2. **Flink 集群模式**：使用 Log4j，与 Flink 集群统一日志管理

## 独立模式（本地开发）

在本地开发和测试时，项目会自动使用 SimpleLogger。

### 配置方式

#### 1. 通过 simplelogger.properties 文件
配置文件位置：`src/main/resources/simplelogger.properties`

```properties
# 设置默认日志级别
org.slf4j.simpleLogger.defaultLogLevel=INFO

# Maruko TableStore 包的日志级别
org.slf4j.simpleLogger.log.org.example.tablestore=DEBUG

# 第三方库日志级别控制
org.slf4j.simpleLogger.log.org.apache.flink=WARN
org.slf4j.simpleLogger.log.org.apache.hadoop=WARN
```

#### 2. 通过系统属性
```bash
# 设置根日志级别
java -Dorg.slf4j.simpleLogger.defaultLogLevel=DEBUG -jar your-app.jar

# 设置特定包的日志级别
java -Dorg.slf4j.simpleLogger.log.org.example.tablestore=TRACE -jar your-app.jar

# 完全禁用调试信息（生产环境）
java -Dorg.slf4j.simpleLogger.defaultLogLevel=WARN -jar your-app.jar
```

#### 3. 通过代码控制
```java
import org.example.tablestore.MarukoLogger;

// 在代码中使用
MarukoLogger.debug("This is a debug message");
MarukoLogger.info("This is an info message");
MarukoLogger.warn("This is a warning message");
MarukoLogger.error("This is an error message");

// 或者获取特定类的logger
import org.slf4j.Logger;
Logger logger = MarukoLogger.getLogger(MyClass.class);
logger.debug("Debug message using SLF4J directly");
```

## Flink 集群模式

在 Flink 集群环境中，Maruko TableStore 会自动继承 Flink 的日志配置。

### 配置方式

#### 1. 通过 Flink 的 log4j 配置文件
在 Flink 的 `conf` 目录下配置 `log4j.properties` 或 `log4j2.xml`：

```properties
# log4j.properties 示例
rootLogger.level = INFO
rootLogger.appenderRef.file.ref = file

# 设置 Maruko TableStore 的日志级别
logger.maruko.name = org.example.tablestore
logger.maruko.level = DEBUG
logger.maruko.appenderRef.file.ref = file
logger.maruko.additivity = false

# 设置第三方库日志级别
logger.flink.name = org.apache.flink
logger.flink.level = WARN
```

#### 2. 通过 Flink 启动参数
```bash
# 在提交 Flink 作业时设置
./bin/flink run -Denv.java.opts="-Dorg.slf4j.simpleLogger.log.org.example.tablestore=DEBUG" your-job.jar
```

#### 3. 通过 Flink 配置文件
在 `flink-conf.yaml` 中添加：
```yaml
env.java.opts: -Dorg.slf4j.simpleLogger.log.org.example.tablestore=DEBUG
```

## 日志级别说明

SLF4J 支持以下日志级别（从详细到简略）：
1. **TRACE** - 最详细的诊断信息
2. **DEBUG** - 调试信息，用于诊断问题
3. **INFO** - 一般信息，记录应用程序的运行状态
4. **WARN** - 警告信息，潜在的问题但不影响程序运行
5. **ERROR** - 错误信息，严重问题需要关注

## 在代码中使用日志

### 方式一：使用 MarukoLogger（推荐）
```java
import org.example.tablestore.MarukoLogger;

public class MyTableStoreClass {
    public void processData() {
        MarukoLogger.info("Starting data processing");
        MarukoLogger.debug("Processing {} records", recordCount);
        MarukoLogger.warn("High memory usage detected: {}%", memoryUsage);
        MarukoLogger.error("Failed to process record: {}", errorMessage);
    }
}
```

### 方式二：使用标准 SLF4J
```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.example.tablestore.MarukoLogger;

public class MyTableStoreClass {
    private static final Logger logger = MarukoLogger.getLogger(MyTableStoreClass.class);
    
    public void processData() {
        logger.info("Starting data processing");
        logger.debug("Processing {} records", recordCount);
        logger.warn("High memory usage detected: {}%", memoryUsage);
        logger.error("Failed to process record", exception);
    }
}
```

## 生产环境建议

### 独立模式
```bash
# 生产环境推荐设置
java -Dorg.slf4j.simpleLogger.defaultLogLevel=WARN \
     -Dorg.slf4j.simpleLogger.log.org.example.tablestore=INFO \
     -jar your-application.jar
```

### Flink 集群模式
在 Flink 的 `log4j.properties` 中设置：
```properties
# 设置根日志级别为 WARN 减少输出
rootLogger.level = WARN

# 设置 Maruko 的日志级别为 INFO
logger.maruko.name = org.example.tablestore
logger.maruko.level = INFO
```

## 开发环境建议

### 本地测试
```bash
# 开发环境调试设置
java -Dorg.slf4j.simpleLogger.defaultLogLevel=DEBUG \
     -Dorg.slf4j.simpleLogger.showDateTime=true \
     -jar your-application.jar
```

## 故障排除

如果日志输出不符合预期，请检查：

1. **配置文件位置**：确保 `simplelogger.properties` 在 classpath 中
2. **系统属性优先级**：系统属性会覆盖配置文件设置
3. **Flink 集成**：在 Flink 集群中，确保使用 Flink 的日志配置
4. **类路径问题**：确保没有多个 SLF4J 绑定冲突

## 注意事项

1. **不要在代码中硬编码日志级别控制**：使用配置文件或系统属性控制
2. **避免昂贵的日志操作**：对于复杂的日志内容，使用条件检查：
   ```java
   if (logger.isDebugEnabled()) {
       logger.debug("Expensive operation result: {}", expensiveToString());
   }
   ```
3. **统一日志格式**：保持日志消息的一致性和可读性
4. **敏感信息保护**：不要在日志中输出密码、密钥等敏感信息