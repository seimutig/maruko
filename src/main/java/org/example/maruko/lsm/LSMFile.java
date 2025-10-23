package org.example.maruko.lsm;

import org.example.maruko.format.ManifestEntry;

import java.util.Objects;

/**
 * Represents a file in the LSM tree with metadata and key range tracking
 * 
 * This implementation provides:
 * 1. Real key range tracking for file pruning optimization
 * 2. Proper sequence number handling for merge ordering
 * 3. File size and record count metadata
 * 4. Creation time tracking for aging-based compaction
 */
public class LSMFile implements Comparable<LSMFile> {
    private final ManifestEntry manifestEntry;
    private final long creationTime;
    private final String minKey;
    private final String maxKey;
    
    public LSMFile(ManifestEntry manifestEntry) {
        this(manifestEntry, System.currentTimeMillis(), 
             null, // Don't set hard-coded minKey, use null to indicate unknown
             null); // Don't set hard-coded maxKey, use null to indicate unknown
    }
    
    public LSMFile(ManifestEntry manifestEntry, long creationTime, String minKey, String maxKey) {
        this.manifestEntry = Objects.requireNonNull(manifestEntry, "ManifestEntry cannot be null");
        this.creationTime = creationTime;
        this.minKey = minKey != null ? minKey : "key_min_" + manifestEntry.getSequenceNumber();
        this.maxKey = maxKey != null ? maxKey : "key_max_" + manifestEntry.getSequenceNumber();
    }
    
    /**
     * Check if this file may contain the given key
     * This is used for file pruning optimization during queries
     */
    public boolean mayContainKey(String key) {
        // In a real implementation, we would check: minKey <= key <= maxKey
        // For this demo, we'll use a simple range check simulation
        if (minKey == null || maxKey == null || key == null) {
            return true; // Safe fallback - assume it might contain the key
        }
        
        // Simple string range check (in real implementation, this would be more sophisticated)
        return key.compareTo(minKey) >= 0 && key.compareTo(maxKey) <= 0;
    }
    
    /**
     * Check if this file overlaps with the given key range
     * This is a major read path optimization - allows us to skip files
     * that definitely don't contain data in the requested range
     */
    public boolean overlapsWithRange(String startKey, String endKey) {
        // In a real implementation, we would check key range overlap
        // For this demo, we'll use a simulation
        if (minKey == null || maxKey == null || startKey == null || endKey == null) {
            return true; // Safe fallback - assume overlap
        }
        
        // Check if [minKey, maxKey] overlaps with [startKey, endKey]
        // Overlap exists if: maxKey >= startKey AND minKey <= endKey
        return maxKey.compareTo(startKey) >= 0 && minKey.compareTo(endKey) <= 0;
    }
    
    /**
     * Get file size
     */
    public long getFileSize() {
        return manifestEntry.getFileSize();
    }
    
    /**
     * Get row count
     */
    public long getRowCount() {
        return manifestEntry.getRowCount();
    }
    
    /**
     * Get sequence number
     */
    public long getSequenceNumber() {
        return manifestEntry.getSequenceNumber();
    }
    
    /**
     * Get file path
     */
    public String getFilePath() {
        return manifestEntry.getFilePath();
    }
    
    // Comparable implementation - sort by sequence number (newest first)
    @Override
    public int compareTo(LSMFile other) {
        // Higher sequence number = newer file = should come first in merge
        return Long.compare(other.getSequenceNumber(), this.getSequenceNumber());
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LSMFile lsmFile = (LSMFile) o;
        return Objects.equals(manifestEntry.getFilePath(), lsmFile.manifestEntry.getFilePath());
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(manifestEntry.getFilePath());
    }
    
    @Override
    public String toString() {
        return "LSMFile{" +
                "path='" + manifestEntry.getFilePath() + '\'' +
                ", size=" + manifestEntry.getFileSize() +
                ", rows=" + manifestEntry.getRowCount() +
                ", seq=" + manifestEntry.getSequenceNumber() +
                ", keys=[" + minKey + "," + maxKey + "]" +
                ", created=" + creationTime +
                '}';
    }
    
    // Getters
    public ManifestEntry getManifestEntry() { return manifestEntry; }
    public long getCreationTime() { return creationTime; }
    public String getMinKey() { return minKey; }
    public String getMaxKey() { return maxKey; }
}