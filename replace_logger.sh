#!/bin/bash

# 替换System.out.println为LoggerUtil调用的脚本

# 备份原始文件
find src/main/java -name "*.java" -exec cp {} {}.bak \;

# 替换System.out.println调用
# 1. 替换简单的字符串输出
find src/main/java -name "*.java" -exec sed -i '' 's/System\.out\.println(\([^)]*\));/LoggerUtil.info(\1);/g' {} \;

# 2. 替换带连接符的字符串输出
find src/main/java -name "*.java" -exec sed -i '' 's/System\.out\.println(\([^+]*\)+\([^)]*\));/LoggerUtil.info(\1 + \2);/g' {} \;

# 3. 替换复杂的输出
find src/main/java -name "*.java" -exec sed -i '' 's/System\.out\.println(\([^)]*\));/LoggerUtil.info(\1);/g' {} \;

echo "替换完成"