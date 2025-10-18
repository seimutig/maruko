package org.example.tablestore.core;

/**
 * Enum representing different types of streaming changes
 * Compatible with Flink's changelog format
 */
public enum StreamChangeType {
    INSERT,          // +I - Initial insert
    UPDATE_BEFORE,   // -U - Before update (old value)
    UPDATE_AFTER,    // +U - After update (new value) 
    DELETE           // -D - Delete operation
}