package org.example.tablestore.lsm;

import java.util.HashMap;
import java.util.Map;

/**
 * Statistics about the LSM tree
 */
public class LSMTreeStats {
    private final Map<Integer, LevelStats> levelStats;
    private final long totalSize;
    private final long totalFiles;
    
    private LSMTreeStats(Builder builder) {
        this.levelStats = new HashMap<>(builder.levelStats);
        this.totalSize = builder.totalSize;
        this.totalFiles = builder.totalFiles;
    }
    
    public static class Builder {
        private final Map<Integer, LevelStats> levelStats = new HashMap<>();
        private long totalSize = 0;
        private long totalFiles = 0;
        
        public Builder addLevelStats(int level, int fileCount, long size) {
            levelStats.put(level, new LevelStats(fileCount, size));
            totalSize += size;
            totalFiles += fileCount;
            return this;
        }
        
        public LSMTreeStats build() {
            return new LSMTreeStats(this);
        }
    }
    
    public static class LevelStats {
        private final int fileCount;
        private final long size;
        
        public LevelStats(int fileCount, long size) {
            this.fileCount = fileCount;
            this.size = size;
        }
        
        // Getters
        public int getFileCount() { return fileCount; }
        public long getSize() { return size; }
    }
    
    // Getters
    public Map<Integer, LevelStats> getLevelStats() { return new HashMap<>(levelStats); }
    public long getTotalSize() { return totalSize; }
    public long getTotalFiles() { return totalFiles; }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LSMTreeStats{");
        sb.append("totalSize=").append(totalSize).append(" bytes, ");
        sb.append("totalFiles=").append(totalFiles).append(", ");
        sb.append("levels={");
        
        boolean first = true;
        for (Map.Entry<Integer, LevelStats> entry : levelStats.entrySet()) {
            if (!first) sb.append(", ");
            LevelStats stats = entry.getValue();
            sb.append("L").append(entry.getKey())
              .append(":").append(stats.getFileCount())
              .append(" files/").append(stats.getSize()).append(" bytes");
            first = false;
        }
        sb.append("}}");
        
        return sb.toString();
    }
}