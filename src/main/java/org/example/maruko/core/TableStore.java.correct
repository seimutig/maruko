package org.example.tablestore.core;

import org.example.tablestore.compaction.CompactionManager;
import org.example.tablestore.format.Snapshot;
import org.example.tablestore.format.ManifestEntry;
import org.example.tablestore.format.ManifestManager;
import org.example.tablestore.format.SnapshotManager;
import org.example.tablestore.io.FileStore;
import org.example.tablestore.lsm.LSMTree;
import org.example.tablestore.lsm.LSMTreeConfig;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

public class TableStore {
    private final String tablePath;
    private final FileStore fileStore;
    private final SnapshotManager snapshotManager;
    private final ManifestManager manifestManager;
    private final MergeOnReadManager mergeManager;
    private final CompactionManager compactionManager;
    private final BucketAssigner bucketAssigner;
    private final List<String> primaryKeyFields;
    private final List<String> partitionFields;
    private final LSMTree lsmTree; // New LSM tree component
    private final ReentrantReadWriteLock tableLock = new ReentrantReadWriteLock();
    private volatile boolean isClosed = false;
    
    // Fields for streaming functionality
    private volatile long lastReadSnapshotId = -1;
    private final List<TableStoreChangeListener> changeListeners = new CopyOnWriteArrayList<>();
    
    // Store for tracking the current state to detect changes in real-time
    private volatile Map<String, Map<String, Object>> currentState = new ConcurrentHashMap<>();
    private final Object stateUpdateLock = new Object();
    
    // Changelog stored as simple append-only log file
    private final String changelogDir;
    private final String changelogFilePath;
    private volatile long lastProcessedChangelogId = 0;  // Track which changes have been consumed by streaming reader

    public TableStore(String tablePath, List<String> primaryKeyFields, List<String> partitionFields, 
                     int numBuckets, org.apache.parquet.schema.MessageType schema) throws IOException {
        // Validate inputs
        if (tablePath == null || tablePath.trim().isEmpty()) {
            throw new IllegalArgumentException("Table path cannot be null or empty");
        }
        if (primaryKeyFields == null) {
            throw new IllegalArgumentException("Primary key fields cannot be null");
        }
        if (numBuckets <= 0) {
            throw new IllegalArgumentException("Number of buckets must be positive");
        }
        
        this.tablePath = tablePath;
        this.primaryKeyFields = primaryKeyFields;
        this.partitionFields = partitionFields != null ? partitionFields : new ArrayList<>();
        this.bucketAssigner = new BucketAssigner(numBuckets);
        
        // Initialize core components
        this.fileStore = new FileStore(tablePath, schema);
        this.snapshotManager = new SnapshotManager(tablePath, fileStore.getFileSystem());
        this.manifestManager = new ManifestManager(tablePath, fileStore.getFileSystem());
        this.mergeManager = new MergeOnReadManager(fileStore, primaryKeyFields);
        this.compactionManager = new CompactionManager(fileStore, manifestManager, mergeManager);
        
        // Initialize LSM tree with default configuration
        LSMTreeConfig lsmConfig = LSMTreeConfig.getDefault();
        this.lsmTree = new LSMTree(tablePath, fileStore, lsmConfig);
        
        // Initialize changelog directory and file
        this.changelogDir = tablePath + "/changelog";
        this.changelogFilePath = changelogDir + "/changelog.log";
        
        // Initialize last read snapshot ID and load initial state
        try {
            Snapshot latestSnapshot = snapshotManager.getLatestSnapshot();
            if (latestSnapshot != null) {
                this.lastReadSnapshotId = latestSnapshot.getId();
                
                // Load the initial state from the latest snapshot
                List<Map<String, Object>> initialState = read();
                synchronized (stateUpdateLock) {
                    currentState.clear();
                    for (Map<String, Object> record : initialState) {
                        if (!primaryKeyFields.isEmpty() && record.containsKey(primaryKeyFields.get(0))) {
                            String key = record.get(primaryKeyFields.get(0)).toString();
                            currentState.put(key, new HashMap<>(record));
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Warning: Could not initialize last read snapshot ID: " + e.getMessage());
            this.lastReadSnapshotId = -1;
        }
    }

    /**
     * Writes records to the table with proper partitioning and bucketing
     * Uses LSM-tree for file organization
     * Determines operation type (INSERT/UPDATE) in real-time and adds to persistent changelog
     */
    public void write(List<Map<String, Object>> records) throws IOException {
        if (isClosed) {
            throw new IllegalStateException("TableStore is closed");
        }
        if (records == null || records.isEmpty()) {
            return; // Nothing to write
        }
        
        tableLock.writeLock().lock();
        try {
            // Validate records
            validateRecords(records);
            
            // Process each record to determine operation type and add to persistent changelog
            for (Map<String, Object> record : records) {
                String primaryKeyValue = null;
                if (!primaryKeyFields.isEmpty() && record.containsKey(primaryKeyFields.get(0))) {
                    primaryKeyValue = record.get(primaryKeyFields.get(0)).toString();
                }
                
                if (primaryKeyValue != null) {
                    // Use in-memory current state to check if record already exists (much more efficient than reading snapshot)
                    Map<String, Object> existingRecord = currentState.get(primaryKeyValue);
                    
                    if (existingRecord != null) {
                        // This is an UPDATE operation
                        ChangeRecord updateRecord = new ChangeRecord(ChangeType.UPDATE, record, existingRecord);
                        appendToChangelog(updateRecord);
                        System.out.println("Persistent Changelog: Detected UPDATE for key " + primaryKeyValue);
                        
                        // Update the in-memory state
                        synchronized (stateUpdateLock) {
                            currentState.put(primaryKeyValue, new HashMap<>(record));
                        }
                    } else {
                        // This is an INSERT operation
                        ChangeRecord insertRecord = new ChangeRecord(ChangeType.INSERT, record, null);
                        appendToChangelog(insertRecord);
                        System.out.println("Persistent Changelog: Detected INSERT for key " + primaryKeyValue);
                        
                        // Update the in-memory state
                        synchronized (stateUpdateLock) {
                            currentState.put(primaryKeyValue, new HashMap<>(record));
                        }
                    }
                } else {
                    // If no primary key, treat as INSERT
                    ChangeRecord insertRecord = new ChangeRecord(ChangeType.INSERT, record, null);
                    appendToChangelog(insertRecord);
                    System.out.println("Persistent Changelog: Detected INSERT (no primary key)");
                }
            }
            
            // Group records by partition and bucket for storage in LSM tree
            Map<PartitionBucketKey, List<Map<String, Object>>> groupedRecords = groupRecordsByPartitionBucket(records);
            
            // Write each group to appropriate partition and bucket (for storage)
            for (Map.Entry<PartitionBucketKey, List<Map<String, Object>>> entry : groupedRecords.entrySet()) {
                PartitionBucketKey key = entry.getKey();
                List<Map<String, Object>> recordsForBucket = entry.getValue();
                
                // Add sequence number for deduplication
                long sequenceNumber = System.currentTimeMillis();
                for (Map<String, Object> record : recordsForBucket) {
                    record.put("_sequence_number", sequenceNumber++);
                }
                
                // Get the actual file path returned by writeData
                String actualFilePath = fileStore.writeData(recordsForBucket, key.getPartitionSpec(), key.getBucket());
                
                // Create manifest entry for the file created by FileStore
                ManifestEntry manifestEntry = new ManifestEntry(
                    ManifestEntry.ADD,
                    sequenceNumber, // Use the sequence number as the base
                    System.currentTimeMillis(), // file identifier
                    buildPartitionPath(key.getPartitionSpec()),
                    key.getBucket(),
                    actualFilePath,
                    0, // size - would be actual file size in real implementation
                    recordsForBucket.size() // row count
                );
                
                // Add file to LSM tree (level 0)
                lsmTree.addFile(manifestEntry);
            }
            
            // Notify change listeners that new data has been written
            // NOTE: We don't create snapshots on every write anymore for production efficiency
            // Snapshots will be created based on Flink checkpoints instead
            notifyChangeListeners(records);
            
        } finally {
            tableLock.writeLock().unlock();
        }
    }
    
    /** 
     * Creates snapshot based on checkpoint - this should be called during Flink checkpointing
     * Clears the real-time changelog as operations are already tracked in real-time
     */
    public void checkpoint() throws IOException {
        if (isClosed) {
            throw new IllegalStateException("TableStore is closed");
        }
        
        tableLock.writeLock().lock();
        try {
            System.out.println("Creating snapshot during checkpoint...");
            
            // Create the actual snapshot with current data
            createSnapshotAndManifest();
            
            // In the real-time changelog model, operations are already tracked when they happen
            // We could optionally clear the changelog buffer after a checkpoint if needed
            // But typically in real-time changelog systems, the changelog persists for readers
            
        } finally {
            tableLock.writeLock().unlock();
        }
    }
    
    /**
     * Gets the current table state as a map keyed by primary key
     */
    private Map<String, Map<String, Object>> getCurrentStateAsMap() throws IOException {
        List<Map<String, Object>> currentState = read();
        Map<String, Map<String, Object>> stateMap = new HashMap<>();
        
        if (!primaryKeyFields.isEmpty()) {
            String primaryKeyField = primaryKeyFields.get(0);
            for (Map<String, Object> record : currentState) {
                if (record.containsKey(primaryKeyField)) {
                    Object key = record.get(primaryKeyField);
                    if (key != null) {
                        stateMap.put(key.toString(), record);
                    }
                }
            }
        }
        
        return stateMap;
    }
    
    /**
     * Validates records before writing
     */
    private void validateRecords(List<Map<String, Object>> records) throws IOException {
        for (Map<String, Object> record : records) {
            if (record == null) {
                throw new IOException("Record cannot be null");
            }
            
            // Check that primary key fields exist and have values
            for (String primaryKeyField : primaryKeyFields) {
                if (!record.containsKey(primaryKeyField) || record.get(primaryKeyField) == null) {
                    throw new IOException("Primary key field '" + primaryKeyField + "' is required and cannot be null");
                }
            }
            
            // Check partition fields
            for (String partitionField : partitionFields) {
                if (!record.containsKey(partitionField)) {
                    System.out.println("Warning: Partition field '" + partitionField + "' not found in record, using default");
                }
            }
        }
    }
    
    /**
     * Reads records from the table applying merge-on-read deduplication
     * Follows the correct path: snapshot -> manifest -> parquet files
     */
    public List<Map<String, Object>> read() throws IOException {
        if (isClosed) {
            throw new IllegalStateException("TableStore is closed");
        }
        
        tableLock.readLock().lock();
        try {
            // Follow the correct read path: get latest snapshot, then manifest entries, then parquet files
            Snapshot latestSnapshot = snapshotManager.getLatestSnapshot();
            
            if (latestSnapshot == null) {
                System.out.println("No snapshots found, returning empty result");
                return new ArrayList<>(); // No data to read if no snapshots exist
            }
            
            System.out.println("Reading from snapshot ID: " + latestSnapshot.getId() + 
                             " with manifest list: " + latestSnapshot.getManifestList());
            
            // Read manifest entries from the snapshot's manifest list
            List<ManifestEntry> manifestEntries = manifestManager.readManifestFile(latestSnapshot.getManifestList());
            
            // Apply merge-on-read deduplication
            List<Map<String, Object>> result = mergeManager.readWithMerge(manifestEntries);
            
            // Update in-memory state to keep it in sync with the latest snapshot
            synchronized (stateUpdateLock) {
                currentState.clear();
                for (Map<String, Object> record : result) {
                    if (!primaryKeyFields.isEmpty() && record.containsKey(primaryKeyFields.get(0))) {
                        String key = record.get(primaryKeyFields.get(0)).toString();
                        currentState.put(key, new HashMap<>(record));
                    }
                }
            }
            
            return result;
        } finally {
            tableLock.readLock().unlock();
        }
    }
    
    /**
     * Reads new change records from the persistent changelog file since last call
     * Returns records with change type information (INSERT, UPDATE, DELETE)
     * Provides real-time changelog operations from persistent storage, without duplicates
     */
    public List<ChangeRecord> readChangeRecords() throws IOException {
        if (isClosed) {
            throw new IllegalStateException("TableStore is closed");
        }
        
        tableLock.readLock().lock();
        try {
            // Check if we have a snapshot or not
            Snapshot latestSnapshot = snapshotManager.getLatestSnapshot();
            if (latestSnapshot == null) {
                // No checkpoint has been performed yet, return new changelog operations from persistent file
                System.out.println("No snapshot exists yet, reading new changelog records from persistent file since position " + 
                                 lastProcessedChangelogId);
                
                // Read new records from the persistent changelog file since last processed position
                List<ChangeRecord> recordsToReturn = readNewChangelogRecords(lastProcessedChangelogId);
                
                // Update the last processed position to prevent duplicates
                long newPosition = getChangelogPosition();
                lastProcessedChangelogId = newPosition;
                
                System.out.println("Returning " + recordsToReturn.size() + " new persistent changelog operations (no checkpoint yet)");
                return recordsToReturn;
            }
            
            System.out.println("Reading new changelog records from persistent file since position " + lastProcessedChangelogId);
            
            // Read new records from the persistent changelog file since last processed position
            List<ChangeRecord> recordsToReturn = readNewChangelogRecords(lastProcessedChangelogId);
            
            // Update the last processed position to prevent duplicates
            long newPosition = getChangelogPosition();
            lastProcessedChangelogId = newPosition;
            
            System.out.println("Returning " + recordsToReturn.size() + " new persistent changelog operations");
            return recordsToReturn;
            
        } finally {
            tableLock.readLock().unlock();
        }
    }
    
    /**
     * Helper method to compare two records for equality, ignoring metadata fields
     */
    private boolean recordsEqual(Map<String, Object> record1, Map<String, Object> record2) {
        if (record1 == null && record2 == null) return true;
        if (record1 == null || record2 == null) return false;
        
        // Compare all business fields (excluding metadata like _sequence_number)
        Set<String> allKeys = new HashSet<>();
        allKeys.addAll(record1.keySet());
        allKeys.addAll(record2.keySet());
        
        for (String key : allKeys) {
            // Skip metadata fields that don't represent actual data changes
            if (!key.equals("_sequence_number") && !key.equals("_version")) {
                Object value1 = record1.get(key);
                Object value2 = record2.get(key);
                
                if (value1 == null && value2 == null) continue;
                if (value1 == null || value2 == null) return false;
                if (!value1.equals(value2)) return false;
            }
        }
        return true;
    }
    
    /**
     * Helper method to extract key business fields for display purposes
     */
    private Map<String, Object> extractKeyFields(Map<String, Object> record) {
        Map<String, Object> keyFields = new HashMap<>();
        for (Map.Entry<String, Object> entry : record.entrySet()) {
            // Skip metadata fields for cleaner display
            if (!entry.getKey().equals("_sequence_number") && !entry.getKey().equals("_version")) {
                keyFields.put(entry.getKey(), entry.getValue());
            }
        }
        return keyFields;
    }
    
    /**
     * Reads only new records since the last read operation (preserving backward compatibility)
     * Returns all new records without considering change types
     */
    public List<Map<String, Object>> readNewChanges() throws IOException {
        List<ChangeRecord> changeRecords = readChangeRecords();
        List<Map<String, Object>> newRecords = new ArrayList<>();
        
        for (ChangeRecord changeRecord : changeRecords) {
            if (changeRecord.getNewValue() != null) {
                // Add the new values to the output
                newRecords.add(changeRecord.getNewValue());
            }
        }
        
        return newRecords;
    }
    
    /**
     * Performs compaction on the table using LSM-tree
     */
    public void compact() throws IOException {
        if (isClosed) {
            throw new IllegalStateException("TableStore is closed");
        }
        
        tableLock.writeLock().lock();
        try {
            System.out.println("Performing LSM-tree compaction on table: " + tablePath);
            // Use LSM tree for compaction
            lsmTree.forceFullCompaction();
            
            // Also run traditional compaction manager for backward compatibility
            // In a real implementation, we might only use the LSM tree approach
            // For this toy implementation, we'll skip calling compactionManager methods
            // since they require specific partition/bucket parameters
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    /**
     * Groups records by their partition and bucket
     */
    private Map<PartitionBucketKey, List<Map<String, Object>>> groupRecordsByPartitionBucket(
            List<Map<String, Object>> records) {
        
        Map<PartitionBucketKey, List<Map<String, Object>>> groupedRecords = new java.util.concurrent.ConcurrentHashMap<>();
        
        for (Map<String, Object> record : records) {
            // Build partition spec
            Map<String, String> partitionSpec = new java.util.HashMap<>();
            for (String partitionField : partitionFields) {
                Object value = record.get(partitionField);
                if (value != null) {
                    partitionSpec.put(partitionField, value.toString());
                } else {
                    partitionSpec.put(partitionField, "default"); // Handle null partition values
                }
            }
            
            // Get bucket - use primary key fields for bucket assignment
            int bucket = bucketAssigner.assignBucketFromFields(primaryKeyFields, record);
            
            // Create key
            PartitionBucketKey key = new PartitionBucketKey(partitionSpec, bucket);
            
            // Add to group - use computeIfAbsent for thread safety
            groupedRecords.computeIfAbsent(key, k -> new java.util.ArrayList<>()).add(record);
        }
        
        return groupedRecords;
    }

    /**
     * Creates snapshot and manifest entries for the written data
     * Now uses LSM tree files instead of accumulating all entries
     */
    private void createSnapshotAndManifest() throws IOException {
        tableLock.writeLock().lock();
        try {
            // Get all files from LSM tree for this snapshot
            List<ManifestEntry> allEntries = lsmTree.getAllFiles();
            
            if (allEntries != null && !allEntries.isEmpty()) {
                // Create a manifest file with the entries
                String manifestFileName = manifestManager.writeManifestFile(allEntries);
                
                // Create snapshot with the manifest
                Snapshot snapshot = snapshotManager.createSnapshot(manifestFileName, 1, "tablestore");
                
                // Optional: Update the last read snapshot ID to avoid reading the same snapshot again
                // This should only be done if the snapshot is not being read as part of streaming
            } else {
                // Create empty manifest if no entries
                String manifestFileName = "manifest_" + System.currentTimeMillis() + ".json";
                List<ManifestEntry> emptyEntries = new java.util.ArrayList<>();
                manifestManager.writeManifestFile(emptyEntries);
                Snapshot snapshot = snapshotManager.createSnapshot(manifestFileName, 1, "tablestore");
            }
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    public String getTablePath() {
        return tablePath;
    }

    public FileStore getFileStore() {
        return fileStore;
    }

    public SnapshotManager getSnapshotManager() {
        return snapshotManager;
    }

    public ManifestManager getManifestManager() {
        return manifestManager;
    }

    public MergeOnReadManager getMergeManager() {
        return mergeManager;
    }

    public CompactionManager getCompactionManager() {
        return compactionManager;
    }
    
    /**
     * Get LSM tree for testing and inspection
     */
    public LSMTree getLSMTree() {
        return lsmTree;
    }
    
    /**
     * Get LSM tree statistics
     */
    public String getLSMTreeStats() {
        return lsmTree.getStats().toString();
    }
    
    /**
     * Closes resources used by this TableStore
     */
    public void close() {
        tableLock.writeLock().lock();
        try {
            if (isClosed) return;
            isClosed = true;
            // In a real implementation, we'd close all resources here
            System.out.println("TableStore closed: " + tablePath);
        } finally {
            tableLock.writeLock().unlock();
        }
    }
    
    /** 
     * Returns whether this TableStore is closed
     */
    public boolean isClosed() {
        return isClosed;
    }
    
    // Streaming functionality methods
    
    /**
     * Sets the last read snapshot ID
     */
    public void setLastReadSnapshotId(long snapshotId) {
        this.lastReadSnapshotId = snapshotId;
    }
    
    /**
     * Gets the last read snapshot ID
     */
    public long getLastReadSnapshotId() {
        return this.lastReadSnapshotId;
    }
    
    /**
     * Add a change listener
     */
    public void addChangeListener(TableStoreChangeListener listener) {
        this.changeListeners.add(listener);
    }
    
    /**
     * Remove a change listener
     */
    public void removeChangeListener(TableStoreChangeListener listener) {
        this.changeListeners.remove(listener);
    }
    
    /**
     * Notify all change listeners about new data
     */
    private void notifyChangeListeners(List<Map<String, Object>> records) {
        for (TableStoreChangeListener listener : changeListeners) {
            try {
                listener.onDataWritten(records);
            } catch (Exception e) {
                System.err.println("Error notifying change listener: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
    
    /**
     * Interface for listening to TableStore changes
     */
    public interface TableStoreChangeListener {
        void onDataWritten(List<Map<String, Object>> records) throws IOException;
    }
    
    /**
     * Helper method to find a record by primary key in a list of records
     */
    private Map<String, Object> findRecordByPrimaryKey(List<Map<String, Object>> records, String primaryKeyValue) {
        if (primaryKeyFields.isEmpty() || primaryKeyValue == null) {
            return null;
        }
        
        String primaryKeyField = primaryKeyFields.get(0);
        for (Map<String, Object> record : records) {
            if (record.containsKey(primaryKeyField)) {
                Object value = record.get(primaryKeyField);
                if (value != null && primaryKeyValue.equals(value.toString())) {
                    return record;
                }
            }
        }
        return null;
    }
    
    /**
     * Appends a change record to the persistent changelog file
     */
    private void appendToChangelog(ChangeRecord changeRecord) throws IOException {
        // Create changelog directory if it doesn't exist
        java.nio.file.Path changelogPath = java.nio.file.Paths.get(changelogDir);
        java.nio.file.Files.createDirectories(changelogPath);
        
        // Write the change record to the changelog file
        java.nio.file.Path logPath = java.nio.file.Paths.get(changelogFilePath);
        
        // Serialize the change record to JSON and append to file
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        String jsonLine = mapper.writeValueAsString(changeRecord) + \"\\n\";
        String jsonLine = mapper.writeValueAsString(changeRecord) + "\n";
        // Append to file using NIO
        java.nio.file.Files.write(logPath, jsonLine.getBytes(\"UTF-8\"), 
        java.nio.file.Files.write(logPath, jsonLine.getBytes("UTF-8"),
                                 java.nio.file.StandardOpenOption.APPEND);
    }
    
    /**
     * Reads new change records from the persistent changelog file since last read position
     */
    private List<ChangeRecord> readNewChangelogRecords(long startPosition) throws IOException {
        java.nio.file.Path logPath = java.nio.file.Paths.get(changelogFilePath);
        
        if (!java.nio.file.Files.exists(logPath)) {
            return new ArrayList<>(); // No changelog file exists yet
        }
        
        // Read all lines from the changelog file
        List<String> allLines = java.nio.file.Files.readAllLines(logPath);
        List<ChangeRecord> newRecords = new ArrayList<>();
        
        // Process lines starting from the specified position
        for (int i = (int)startPosition; i < allLines.size(); i++) {
            String line = allLines.get(i);
            if (line.trim().isEmpty()) continue;
            
            try {
                // Deserialize the JSON line back to ChangeRecord
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                ChangeRecord record = mapper.readValue(line, ChangeRecord.class);
                newRecords.add(record);
            } catch (Exception e) {
                System.err.println(\"Error parsing changelog line: \" + line + \", error: \" + e.getMessage());
            }
        }
        
        return newRecords;
    }
    
    /**
     * Gets the current size/position of the changelog file
     */
    private long getChangelogPosition() throws IOException {
        java.nio.file.Path logPath = java.nio.file.Paths.get(changelogFilePath);
        
        if (!java.nio.file.Files.exists(logPath)) {
            return 0;
        }
        
        // Just return the number of lines as the position
        List<String> allLines = java.nio.file.Files.readAllLines(logPath);
        return allLines.size();
    }
    
    private String buildPartitionPath(Map<String, String> partitionSpec) {
        StringBuilder path = new StringBuilder();
        for (Map.Entry<String, String> entry : partitionSpec.entrySet()) {
            if (path.length() > 0) {
                path.append(\"/\");
            }
            path.append(entry.getKey()).append(\"=\").append(entry.getValue());
        }
        return path.length() > 0 ? path.toString() : \"default\";
    }
    
    /**
     * Change record to track type of data change
     */
    public static class ChangeRecord {
        private final ChangeType changeType;
        private final Map<String, Object> newValue;  // For INSERT/UPDATE
        private final Map<String, Object> oldValue;  // For UPDATE/DELETE
        
        public ChangeRecord(ChangeType changeType, Map<String, Object> newValue, Map<String, Object> oldValue) {
            this.changeType = changeType;
            this.newValue = newValue;
            this.oldValue = oldValue;
        }
        
        public ChangeType getChangeType() {
            return changeType;
        }
        
        public Map<String, Object> getNewValue() {
            return newValue;
        }
        
        public Map<String, Object> getOldValue() {
            return oldValue;
        }
    }
    
    /**
     * Enum for different types of changes
     */
    public enum ChangeType {
        INSERT,
        UPDATE,
        DELETE
    }
}