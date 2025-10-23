package org.example.maruko.format;

import java.util.List;
import java.util.Map;

public class ManifestFileMeta {
    private String fileName;
    private long fileSize;
    private int numAddedFiles;
    private int numDeletedFiles;
    private long schemaId;
    private List<Map<String, String>> partitionSpecs;
    private long minSequenceNumber;
    private long maxSequenceNumber;

    public ManifestFileMeta(String fileName, long fileSize, int numAddedFiles, int numDeletedFiles,
                            long schemaId, List<Map<String, String>> partitionSpecs) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.numAddedFiles = numAddedFiles;
        this.numDeletedFiles = numDeletedFiles;
        this.schemaId = schemaId;
        this.partitionSpecs = partitionSpecs;
        this.minSequenceNumber = 0;
        this.maxSequenceNumber = 0;
    }

    // Getters and setters
    public String getFileName() {
        return fileName;
    }

    public long getFileSize() {
        return fileSize;
    }

    public int getNumAddedFiles() {
        return numAddedFiles;
    }

    public int getNumDeletedFiles() {
        return numDeletedFiles;
    }

    public long getSchemaId() {
        return schemaId;
    }

    public List<Map<String, String>> getPartitionSpecs() {
        return partitionSpecs;
    }

    public void setPartitionSpecs(List<Map<String, String>> partitionSpecs) {
        this.partitionSpecs = partitionSpecs;
    }

    public long getMinSequenceNumber() {
        return minSequenceNumber;
    }

    public void setMinSequenceNumber(long minSequenceNumber) {
        this.minSequenceNumber = minSequenceNumber;
    }

    public long getMaxSequenceNumber() {
        return maxSequenceNumber;
    }

    public void setMaxSequenceNumber(long maxSequenceNumber) {
        this.maxSequenceNumber = maxSequenceNumber;
    }
}