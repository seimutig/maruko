# SLF4J日志控制指南

## 概述

本项目现已迁移到使用SLF4J日志框架，与Flink保持一致。SLF4J提供了更灵活和标准化的日志控制方式。

## 日志级别

SLF4J支持以下日志级别（从低到高）：
1. TRACE - 最详细的信息
2. DEBUG - 调试信息
3. INFO - 一般信息
4. WARN - 警告信息
5. ERROR - 错误信息

## 配置方式

### 1. 通过配置文件

项目默认使用simplelogger.properties配置文件：

```properties
# 设置默认日志级别
org.slf4j.simpleLogger.defaultLogLevel=INFO

# 设置特定包的日志级别
org.slf4j.simpleLogger.log.org.example.tablestore=INFO
org.slf4j.simpleLogger.log.org.apache.flink=WARN

# 显示日期时间
org.slf4j.simpleLogger.showDateTime=true
org.slf4j.simpleLogger.dateTimeFormat=yyyy-MM-dd HH:mm:ss.SSS
```

### 2. 通过系统属性

可以在启动应用时通过JVM参数控制日志级别：

```bash
# 设置根日志级别为WARN
java -Dorg.slf4j.simpleLogger.defaultLogLevel=WARN -jar your-application.jar

# 设置特定包的日志级别
java -Dorg.slf4j.simpleLogger.log.org.example.tablestore=DEBUG -jar your-application.jar

# 完全关闭调试信息
java -Dorg.slf4j.simpleLogger.defaultLogLevel=ERROR -jar your-application.jar
```

### 3. 通过环境变量

也可以通过环境变量设置：

```bash
# Linux/Mac
export JAVA_OPTS="-Dorg.slf4j.simpleLogger.defaultLogLevel=WARN"
mvn exec:java -Dexec.mainClass="org.example.tablestore.YourMainClass"

# Windows
set JAVA_OPTS=-Dorg.slf4j.simpleLogger.defaultLogLevel=WARN
mvn exec:java -Dexec.mainClass="org.example.tablestore.YourMainClass"
```

## 使用示例

### 在代码中使用日志

```java
import org.example.tablestore.DebugLoggerAdapter;

public class MyClass {
    public void myMethod() {
        // 使用DebugLoggerAdapter适配器（向后兼容）
        DebugLoggerAdapter.debug("This is a debug message");
        DebugLoggerAdapter.info("This is an info message");
        DebugLoggerAdapter.warn("This is a warning message");
        DebugLoggerAdapter.error("This is an error message");
    }
}
```

或者直接使用SLF4J：

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyClass {
    private static final Logger logger = LoggerFactory.getLogger(MyClass.class);
    
    public void myMethod() {
        logger.debug("This is a debug message");
        logger.info("This is an info message");
        logger.warn("This is a warning message");
        logger.error("This is an error message");
    }
}
```

## 生产环境建议

在生产环境中，建议将日志级别设置为INFO或WARN以减少日志输出：

```bash
# 生产环境推荐设置
java -Dorg.slf4j.simpleLogger.defaultLogLevel=WARN \
     -Dorg.slf4j.simpleLogger.log.org.example.tablestore=INFO \
     -jar your-application.jar
```

## 开发环境建议

在开发环境中，可以根据需要调整日志级别：

```bash
# 开发环境调试设置
java -Dorg.slf4j.simpleLogger.defaultLogLevel=DEBUG \
     -Dorg.slf4j.simpleLogger.showDateTime=true \
     -jar your-application.jar
```

## 故障排除

如果日志输出过多，可以：

1. 检查simplelogger.properties配置文件是否存在且配置正确
2. 通过系统属性覆盖默认配置
3. 在代码中减少DEBUG级别的日志调用
4. 使用条件日志记录避免不必要的字符串拼接：

```java
// 好的做法
if (logger.isDebugEnabled()) {
    logger.debug("Processing item: {}", expensiveToString(item));
}

// 避免的做法（即使debug级别未启用也会执行expensiveToString）
logger.debug("Processing item: {}", expensiveToString(item));
```