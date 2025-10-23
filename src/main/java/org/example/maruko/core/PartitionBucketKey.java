package org.example.maruko.core;

import java.util.Map;
import java.util.Objects;

/**
 * A key that combines partition specification and bucket for organizing TableStore data
 */
public class PartitionBucketKey {
    private final Map<String, String> partitionSpec;
    private final int bucket;
    
    public PartitionBucketKey(Map<String, String> partitionSpec, int bucket) {
        this.partitionSpec = partitionSpec != null ? partitionSpec : java.util.Collections.emptyMap();
        this.bucket = bucket;
    }
    
    public Map<String, String> getPartitionSpec() {
        return partitionSpec;
    }
    
    public int getBucket() {
        return bucket;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionBucketKey that = (PartitionBucketKey) o;
        return bucket == that.bucket &&
               Objects.equals(partitionSpec, that.partitionSpec);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(partitionSpec, bucket);
    }
    
    @Override
    public String toString() {
        return "PartitionBucketKey{" +
               "partitionSpec=" + partitionSpec +
               ", bucket=" + bucket +
               '}';
    }
}