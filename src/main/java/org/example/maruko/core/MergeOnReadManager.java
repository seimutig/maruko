package org.example.maruko.core;

import org.example.maruko.format.ManifestEntry;
import org.example.maruko.format.Snapshot;
import org.example.maruko.io.IFileStore;
import org.example.maruko.MarukoLogger;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.*;

public class MergeOnReadManager {
    private static final Logger logger = MarukoLogger.getLogger(MergeOnReadManager.class);
    private final IFileStore fileStore;
    private final List<String> primaryKeyFields;
    private final Object mergeLock = new Object(); // Simple lock for merge operations

    public MergeOnReadManager(IFileStore fileStore, List<String> primaryKeyFields) {
        if (fileStore == null) {
            throw new IllegalArgumentException("FileStore cannot be null");
        }
        if (primaryKeyFields == null) {
            throw new IllegalArgumentException("Primary key fields cannot be null");
        }
        
        this.fileStore = fileStore;
        this.primaryKeyFields = primaryKeyFields;
    }

    /**
     * Performs merge-on-read deduplication for records with same primary key
     * Keeps the record with the highest sequence number (most recent)
     * Now properly handles delete operations using tombstone records
     */
    public List<Map<String, Object>> mergeWithDeduplication(List<Map<String, Object>> records) {
        return mergeWithDeduplicationIncludingDeletes(records);
    }
    
    /**
     * Performs merge-on-read deduplication including proper delete handling
     * Keeps the record with the highest sequence number (most recent)
     * Handles tombstone records to properly filter out deleted data
     */
    public List<Map<String, Object>> mergeWithDeduplicationIncludingDeletes(List<Map<String, Object>> records) {
        if (records == null || records.isEmpty()) {
            return new ArrayList<>();
        }

        synchronized (mergeLock) {
            // Track deleted keys and their sequence numbers
            Map<String, Long> deletedKeys = new HashMap<>();
            // Track non-deleted records by primary key
            Map<String, Map<String, Object>> deduplicatedRecords = new LinkedHashMap<>();
            
            logger.debug("DEBUG: Starting merge with deletes, processing {} records", records.size());
            
            // First pass: identify all tombstone records and collect deleted keys
            for (int i = 0; i < records.size(); i++) {
                Map<String, Object> record = records.get(i);
                if (record == null) {
                    logger.debug("DEBUG: Record {} is null, skipping", i);
                    continue; // Skip null records
                }
                
                logger.debug("DEBUG: Processing record {}: {}", i, record);
                
                // Check if this is a tombstone record
                Object isDeleted = record.get("_deleted");
                logger.debug("DEBUG: Record {} _deleted field: {} (type: {})", i, isDeleted, (isDeleted != null ? isDeleted.getClass().getName() : "null"));
                
                if (isDeleted instanceof Boolean && (Boolean) isDeleted) {
                    String primaryKey = buildPrimaryKey(record);
                    Long sequenceNumber = getSequenceNumber(record);
                    
                    logger.debug("DEBUG: Found tombstone record {} for key {} with sequence {}", i, primaryKey, sequenceNumber);
                    
                    // Keep track of deleted keys with their sequence numbers
                    Long existingDeleteSeq = deletedKeys.get(primaryKey);
                    if (existingDeleteSeq == null || sequenceNumber > existingDeleteSeq) {
                        deletedKeys.put(primaryKey, sequenceNumber);
                        logger.debug("DEBUG: Added tombstone for key {} with sequence {}", primaryKey, sequenceNumber);
                    }
                } else {
                    logger.debug("DEBUG: Record {} is not a tombstone record", i);
                }
            }
            
            logger.debug("DEBUG: First pass completed, found {} deleted keys", deletedKeys.size());
            
            // Second pass: process all records, filtering out deleted ones
            for (Map<String, Object> record : records) {
                if (record == null) {
                    continue; // Skip null records
                }
                
                // Skip tombstone records as they've already been processed
                Object isDeleted = record.get("_deleted");
                if (isDeleted instanceof Boolean && (Boolean) isDeleted) {
                    continue;
                }
                
                String primaryKey = buildPrimaryKey(record);
                
                // Check if this record's key has been marked as deleted
                Long deleteSequence = deletedKeys.get(primaryKey);
                if (deleteSequence != null) {
                    // Key has been deleted, check sequence numbers
                    Long recordSequence = getSequenceNumber(record);
                    if (recordSequence != null && recordSequence < deleteSequence) {
                        // Record is older than delete operation, skip it
                        System.out.println("Skipping record with key " + primaryKey + " due to newer tombstone (record seq: " + recordSequence + ", delete seq: " + deleteSequence + ")");
                        continue;
                    }
                    // If record is newer than delete, we'll process it normally below
                }
                
                // Check if we already have a record with this key
                Map<String, Object> existingRecord = deduplicatedRecords.get(primaryKey);
                
                if (existingRecord == null) {
                    // First occurrence of this key
                    deduplicatedRecords.put(primaryKey, record);
                } else {
                    // Compare sequence numbers to keep the most recent
                    Long existingSeq = getSequenceNumber(existingRecord);
                    Long currentSeq = getSequenceNumber(record);
                    
                    if (currentSeq != null && (existingSeq == null || currentSeq > existingSeq)) {
                        deduplicatedRecords.put(primaryKey, record);
                    }
                }
            }
            
            System.out.println("Merge-on-read completed: " + records.size() + " input records -> " + deduplicatedRecords.size() + " output records, " + deletedKeys.size() + " deleted keys");
            
            return new ArrayList<>(deduplicatedRecords.values());
        }
    }
    
    /**
     * Extract sequence number from record with proper null handling
     */
    private Long getSequenceNumber(Map<String, Object> record) {
        if (record == null) return null;
        Object seqObj = record.get("_sequence_number");
        if (seqObj instanceof Long) {
            return (Long) seqObj;
        } else if (seqObj instanceof Number) {
            return ((Number) seqObj).longValue();
        } else if (seqObj instanceof String) {
            try {
                return Long.parseLong((String) seqObj);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    /**
     * Builds a primary key string from the record's primary key fields
     */
    private String buildPrimaryKey(Map<String, Object> record) {
        if (record == null) {
            return "null_record";
        }
        
        StringBuilder keyBuilder = new StringBuilder();
        
        for (String field : primaryKeyFields) {
            Object value = record.get(field);
            if (value != null) {
                keyBuilder.append(value.toString());
            } else {
                keyBuilder.append("null");
            }
            keyBuilder.append("|"); // Delimiter to separate field values
        }
        
        return keyBuilder.toString();
    }

    /**
     * Reads records from multiple files and applies deduplication
     * Now properly handles delete operations using tombstone records
     */
    public List<Map<String, Object>> readWithMerge(List<ManifestEntry> manifestEntries) throws IOException {
        if (manifestEntries == null || manifestEntries.isEmpty()) {
            return new ArrayList<>();
        }
        
        List<Map<String, Object>> allRecords = new ArrayList<>();
        
        for (ManifestEntry entry : manifestEntries) {
            if (entry.getKind() == ManifestEntry.ADD) {
                // Read from the actual data file
                List<Map<String, Object>> fileRecords = readRecordsFromEntry(entry);
                allRecords.addAll(fileRecords);
            } else if (entry.getKind() == ManifestEntry.DELETE) {
                // Handle delete entries in a more sophisticated implementation
                // Read tombstone records from DELETE entries
                List<Map<String, Object>> tombstoneRecords = readRecordsFromEntry(entry);
                allRecords.addAll(tombstoneRecords);
                System.out.println("Processed " + tombstoneRecords.size() + " tombstone records from DELETE entry: " + entry.getFilePath());
            }
        }
        
        // Apply deduplication with tombstone handling
        return mergeWithDeduplicationIncludingDeletes(allRecords);
    }

    /**
     * Reads records from a manifest entry file
     * Now properly reads from the actual Parquet data file
     */
    private List<Map<String, Object>> readRecordsFromEntry(ManifestEntry entry) throws IOException {
        // Read from the actual Parquet data file
        try {
            // Use our SimpleMapParquetReader to read the Parquet file
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(entry.getFilePath());
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            
            List<Map<String, Object>> records = new java.util.ArrayList<>();
            
            try (org.example.maruko.io.SimpleMapParquetReader reader = 
                    new org.example.maruko.io.SimpleMapParquetReader.Builder(path).build(conf)) {
                
                Map<String, Object> record;
                while ((record = reader.read()) != null) {
                    if (record != null) {
                        records.add(record);
                    }
                }
            }
            
            return records;
            
        } catch (Exception e) {
            // If file reading fails, return empty list
            System.out.println("Could not read file: " + entry.getFilePath() + ", error: " + e.getMessage());
            e.printStackTrace(); // Print the stack trace to see the actual error
        }
        
        // Return empty list if file doesn't exist or can't be read
        return new ArrayList<>();
    }
    
    /**
     * Parses a record string back to a Map
     * This is a simplified implementation for the toy project
     */
    private Map<String, Object> parseRecordFromString(String recordStr) {
        if (recordStr == null || recordStr.trim().isEmpty()) {
            return null;
        }
        
        try {
            // Remove the {} brackets and parse key=value pairs
            String cleanStr = recordStr.trim();
            if (cleanStr.startsWith("{") && cleanStr.endsWith("}")) {
                cleanStr = cleanStr.substring(1, cleanStr.length() - 1);
            }
            
            Map<String, Object> record = new java.util.HashMap<>();
            String[] pairs = cleanStr.split(", ");
            
            for (String pair : pairs) {
                String[] keyValue = pair.split("=", 2);
                if (keyValue.length == 2) {
                    String key = keyValue[0].trim();
                    String value = keyValue[1].trim();
                    
                    // Remove quotes if present
                    if (value.startsWith("\"") && value.endsWith("\"") && value.length() > 1) {
                        value = value.substring(1, value.length() - 1);
                    } else if (value.startsWith("'") && value.endsWith("'") && value.length() > 1) {
                        value = value.substring(1, value.length() - 1);
                    }
                    
                    // Try to parse numeric values
                    if (key.contains("timestamp") || key.contains("sequence") || key.contains("_number")) {
                        try {
                            record.put(key, Long.parseLong(value));
                        } catch (NumberFormatException e) {
                            record.put(key, value);
                        }
                    } else if (key.contains("age") || key.contains("id")) {
                        try {
                            record.put(key, Integer.parseInt(value));
                        } catch (NumberFormatException e) {
                            record.put(key, value);
                        }
                    } else if (key.contains("salary") || key.contains("amount")) {
                        try {
                            record.put(key, Double.parseDouble(value));
                        } catch (NumberFormatException e) {
                            record.put(key, value);
                        }
                    } else {
                        record.put(key, value);
                    }
                }
            }
            
            return record;
        } catch (Exception e) {
            // If parsing fails, return a basic record structure
            System.out.println("Warning: Failed to parse record string: " + recordStr);
            return null;
        }
    }

    public List<Map<String, Object>> mergeRecordsForSnapshot(Snapshot snapshot, List<ManifestEntry> entries) 
            throws IOException {
        // Read all records from the manifest entries in the snapshot
        return readWithMerge(entries);
    }
    
    public List<String> getPrimaryKeyFields() {
        return new ArrayList<>(primaryKeyFields);
    }
}