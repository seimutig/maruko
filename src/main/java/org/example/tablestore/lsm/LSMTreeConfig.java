package org.example.tablestore.lsm;

/**
 * Default configuration for LSM-tree
 */
public class LSMTreeConfig {
    // Number of levels in the LSM tree (including level 0)
    private final int numLevels;
    
    // Maximum number of files in level 0 before triggering compaction
    private final int level0MaxFiles;
    
    // Size ratio between consecutive levels (level N size = level N-1 size * sizeRatio)
    private final int sizeRatio;
    
    // Target file size for each level
    private final long[] targetFileSizePerLevel;
    
    // Minimum number of files to trigger compaction
    private final int minCompactFiles;
    
    public LSMTreeConfig() {
        this.numLevels = 4; // L0, L1, L2, L3
        this.level0MaxFiles = 4; // Trigger compaction when L0 has 4 files
        this.sizeRatio = 10; // Each level is 10x bigger than previous
        this.targetFileSizePerLevel = new long[]{64 * 1024 * 1024, 128 * 1024 * 1024, 256 * 1024 * 1024, 512 * 1024 * 1024}; // 64MB, 128MB, 256MB, 512MB
        this.minCompactFiles = 2;
    }
    
    public LSMTreeConfig(int numLevels, int level0MaxFiles, int sizeRatio, 
                        long[] targetFileSizePerLevel, int minCompactFiles) {
        this.numLevels = numLevels;
        this.level0MaxFiles = level0MaxFiles;
        this.sizeRatio = sizeRatio;
        this.targetFileSizePerLevel = targetFileSizePerLevel;
        this.minCompactFiles = minCompactFiles;
    }
    
    // Getters
    public int getNumLevels() { return numLevels; }
    public int getLevel0MaxFiles() { return level0MaxFiles; }
    public int getSizeRatio() { return sizeRatio; }
    public long getTargetFileSize(int level) { 
        return level < targetFileSizePerLevel.length ? targetFileSizePerLevel[level] : targetFileSizePerLevel[targetFileSizePerLevel.length - 1];
    }
    public int getMinCompactFiles() { return minCompactFiles; }
    
    public static LSMTreeConfig getDefault() {
        return new LSMTreeConfig();
    }
}