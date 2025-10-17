package org.example.tablestore.format;

import java.util.Map;

public class ManifestEntry {
    public static final int ADD = 0;
    public static final int DELETE = 1;
    public static final int OVERWRITE = 2;

    private int kind; // 0=add, 1=delete, 2=overwrite
    private long sequenceNumber;
    private long file;
    private String partition;
    private int bucket;
    private String filePath;
    private long fileSize;
    private int rowCount;
    private Map<String, Object> minValues;
    private Map<String, Object> maxValues;
    private Map<String, Object> nullCounts;

    // Default constructor for Jackson deserialization
    public ManifestEntry() {
    }

    public ManifestEntry(int kind, long sequenceNumber, long file, String partition, 
                         int bucket, String filePath, long fileSize, int rowCount) {
        this.kind = kind;
        this.sequenceNumber = sequenceNumber;
        this.file = file;
        this.partition = partition;
        this.bucket = bucket;
        this.filePath = filePath;
        this.fileSize = fileSize;
        this.rowCount = rowCount;
    }

    // Getters
    public int getKind() {
        return kind;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public long getFile() {
        return file;
    }

    public String getPartition() {
        return partition;
    }

    public int getBucket() {
        return bucket;
    }

    public String getFilePath() {
        return filePath;
    }

    public long getFileSize() {
        return fileSize;
    }

    public int getRowCount() {
        return rowCount;
    }

    public Map<String, Object> getMinValues() {
        return minValues;
    }

    public void setMinValues(Map<String, Object> minValues) {
        this.minValues = minValues;
    }

    public Map<String, Object> getMaxValues() {
        return maxValues;
    }

    public void setMaxValues(Map<String, Object> maxValues) {
        this.maxValues = maxValues;
    }

    public Map<String, Object> getNullCounts() {
        return nullCounts;
    }

    public void setNullCounts(Map<String, Object> nullCounts) {
        this.nullCounts = nullCounts;
    }

    public void setKind(int kind) {
        this.kind = kind;
    }

    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public void setFile(long file) {
        this.file = file;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public void setBucket(int bucket) {
        this.bucket = bucket;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }
}