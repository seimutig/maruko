package org.example.maruko.core;

import java.util.Map;

/**
 * Represents a change record in the streaming changelog
 * Used for Flink streaming mode to emit proper +I, -U, +U, -D records
 */
public class StreamChangeRecord {
    private final StreamChangeType changeType;
    private final Map<String, Object> newValue;  // For +I, +U
    private final Map<String, Object> oldValue;  // For -U, -D
    
    public StreamChangeRecord(StreamChangeType changeType, Map<String, Object> newValue, Map<String, Object> oldValue) {
        this.changeType = changeType;
        this.newValue = newValue;
        this.oldValue = oldValue;
    }
    
    public StreamChangeType getChangeType() {
        return changeType;
    }
    
    public Map<String, Object> getNewValue() {
        return newValue;
    }
    
    public Map<String, Object> getOldValue() {
        return oldValue;
    }
    
    @Override
    public String toString() {
        return "StreamChangeRecord{" +
                "changeType=" + changeType +
                ", newValue=" + newValue +
                ", oldValue=" + oldValue +
                '}';
    }
}