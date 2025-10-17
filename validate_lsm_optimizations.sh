#!/bin/bash

# Script to validate that LSM-tree optimizations are working
echo "=== Validating LSM-Tree Optimizations ==="

# Navigate to project directory
cd /Users/xiaoyuan/maruko_project

# Compile the project
echo "Compiling project..."
mvn clean compile

if [ $? -eq 0 ]; then
    echo "✓ Compilation successful"
else
    echo "✗ Compilation failed"
    exit 1
fi

# Run the performance proof test
echo "Running LSM-tree performance proof..."
mvn exec:java -Dexec.mainClass="org.example.tablestore.lsm.LSMTreePerformanceProof"

if [ $? -eq 0 ]; then
    echo "✓ Performance proof completed successfully"
else
    echo "✗ Performance proof failed"
    exit 1
fi

# Run the real optimizations demo
echo "Running real optimizations demo..."
mvn exec:java -Dexec.mainClass="org.example.tablestore.lsm.RealLSMOptimizationsDemo"

if [ $? -eq 0 ]; then
    echo "✓ Real optimizations demo completed successfully"
else
    echo "✗ Real optimizations demo failed"
    exit 1
fi

echo "=== All validations passed! LSM-tree optimizations are working ==="