package org.example.maruko.compaction;

import org.example.maruko.core.MergeOnReadManager;
import org.example.maruko.format.ManifestEntry;
import org.example.maruko.format.ManifestManager;
import org.example.maruko.io.IFileStore;

import java.io.IOException;
import java.util.*;

public class CompactionManager {
    private IFileStore fileStore;
    private ManifestManager manifestManager;
    private MergeOnReadManager mergeManager;
    private int maxFileSizeThreshold; // in bytes

    public CompactionManager(IFileStore fileStore, ManifestManager manifestManager, 
                           MergeOnReadManager mergeManager) {
        this.fileStore = fileStore;
        this.manifestManager = manifestManager;
        this.mergeManager = mergeManager;
        this.maxFileSizeThreshold = 1024 * 1024 * 128; // 128MB default threshold
    }

    /**
     * Performs small file compaction - merges small files in the same bucket
     */
    public void compactSmallFiles(String partition, int bucket) throws IOException {
        // Get all manifest entries for the partition and bucket
        List<ManifestEntry> entries = getManifestEntriesForPartitionBucket(partition, bucket);
        
        // Group small files for compaction
        List<ManifestEntry> smallFiles = new ArrayList<>();
        for (ManifestEntry entry : entries) {
            if (entry.getFileSize() < maxFileSizeThreshold && entry.getKind() == ManifestEntry.ADD) {
                smallFiles.add(entry);
            }
        }
        
        if (smallFiles.size() <= 1) {
            return; // Nothing to compact
        }
        
        // Read and deduplicate records from small files
        List<Map<String, Object>> allRecords = mergeManager.readWithMerge(smallFiles);
        
        if (allRecords.isEmpty()) {
            return; // Nothing to compact
        }
        
        // Write compacted data to a new file
        Map<String, String> partitionSpec = parsePartitionSpec(partition);
        fileStore.writeData(allRecords, partitionSpec, bucket);
        
        // Update manifest to mark old files as deleted and add the new file
        updateManifestForCompaction(smallFiles, partition, bucket);
    }

    /**
     * Performs major compaction - full deduplication of all files in a partition-bucket
     */
    public void compactMajor(String partition, int bucket) throws IOException {
        // Get all manifest entries for the partition and bucket
        List<ManifestEntry> entries = getManifestEntriesForPartitionBucket(partition, bucket);
        
        // Filter to ADD entries only
        List<ManifestEntry> addEntries = new ArrayList<>();
        for (ManifestEntry entry : entries) {
            if (entry.getKind() == ManifestEntry.ADD) {
                addEntries.add(entry);
            }
        }
        
        if (addEntries.isEmpty()) {
            return; // Nothing to compact
        }
        
        // Read and fully deduplicate all records
        List<Map<String, Object>> allRecords = mergeManager.readWithMerge(addEntries);
        
        if (allRecords.isEmpty()) {
            return; // Nothing to compact
        }
        
        // Write compacted data to a new file
        Map<String, String> partitionSpec = parsePartitionSpec(partition);
        fileStore.writeData(allRecords, partitionSpec, bucket);
        
        // Update manifest to mark all old files as deleted and add the new file
        updateManifestForMajorCompaction(entries, partition, bucket);
    }

    /**
     * Gets all manifest entries for a specific partition and bucket
     */
    private List<ManifestEntry> getManifestEntriesForPartitionBucket(String partition, int bucket) throws IOException {
        List<ManifestEntry> allEntries = new ArrayList<>();
        List<String> manifestFiles = manifestManager.getAllManifestFiles();
        
        for (String fileName : manifestFiles) {
            List<ManifestEntry> entries = manifestManager.readManifestFile(fileName);
            for (ManifestEntry entry : entries) {
                if (partition.equals(entry.getPartition()) && bucket == entry.getBucket()) {
                    allEntries.add(entry);
                }
            }
        }
        
        return allEntries;
    }

    /**
     * Updates manifest entries after small file compaction
     */
    private void updateManifestForCompaction(List<ManifestEntry> oldEntries, String partition, int bucket) throws IOException {
        // In a real implementation, we would mark old files as deleted and add new file
        // For this toy implementation, as a minimum, we'll log the action
        System.out.println("CompactSmallFiles: Compacted " + oldEntries.size() + " files in partition=" + 
                          partition + ", bucket=" + bucket);
    }

    /**
     * Updates manifest entries after major compaction
     */
    private void updateManifestForMajorCompaction(List<ManifestEntry> oldEntries, String partition, int bucket) throws IOException {
        // In a real implementation, we would mark all old files as deleted and add new file
        // For this toy implementation, as a minimum, we'll log the action
        System.out.println("CompactMajor: Compacted " + oldEntries.size() + " files in partition=" + 
                          partition + ", bucket=" + bucket);
    }

    /**
     * Parses partition specification from string format (e.g., "date=2023-01-01/hour=12")
     */
    private Map<String, String> parsePartitionSpec(String partition) {
        Map<String, String> spec = new HashMap<>();
        if (partition != null && !partition.isEmpty()) {
            String[] parts = partition.split("/");
            for (String part : parts) {
                String[] kv = part.split("=", 2);
                if (kv.length == 2) {
                    spec.put(kv[0], kv[1]);
                }
            }
        }
        return spec;
    }

    /**
     * Performs time-based compaction based on file age
     */
    public void compactTimeBased(String partition, int bucket, long maxFileAgeMs) throws IOException {
        // Get all manifest entries for the partition and bucket
        List<ManifestEntry> entries = getManifestEntriesForPartitionBucket(partition, bucket);
        
        // Filter files based on age
        List<ManifestEntry> oldFiles = new ArrayList<>();
        for (ManifestEntry entry : entries) {
            // In a real implementation, we would check the file modification time
            // For this toy implementation, we'll assume all files are candidates
            if (entry.getKind() == ManifestEntry.ADD) {
                oldFiles.add(entry);
            }
        }
        
        if (oldFiles.size() <= 1) {
            return; // Nothing to compact
        }
        
        // Perform compaction on old files
        List<Map<String, Object>> allRecords = mergeManager.readWithMerge(oldFiles);
        
        if (allRecords.isEmpty()) {
            return; // Nothing to compact
        }
        
        // Write compacted data to a new file
        Map<String, String> partitionSpec = parsePartitionSpec(partition);
        fileStore.writeData(allRecords, partitionSpec, bucket);
        
        // Update manifest
        updateManifestForCompaction(oldFiles, partition, bucket);
    }
}