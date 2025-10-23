package org.example.maruko.core;

import java.util.Map;
import java.util.List;

public class BucketAssigner {
    private int numBuckets;

    public BucketAssigner(int numBuckets) {
        this.numBuckets = numBuckets;
    }

    public int assignBucket(String primaryKey) {
        if (primaryKey == null) {
            return 0; // Default bucket for null primary key
        }
        int hash = primaryKey.hashCode();
        return Math.abs(hash) % numBuckets;
    }

    public int assignBucketFromFields(List<String> primaryKeyFields, Map<String, Object> record) {
        StringBuilder keyBuilder = new StringBuilder();
        for (String field : primaryKeyFields) {
            Object value = record.get(field);
            if (value != null) {
                keyBuilder.append(value.toString());
            } else {
                keyBuilder.append("null");
            }
        }
        return assignBucket(keyBuilder.toString());
    }
}