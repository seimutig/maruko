package org.example.tablestore.format;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Manages snapshots for the TableStore
 */
public class SnapshotManager {
    private final String tablePath;
    private final FileSystem fileSystem;
    private final String snapshotDir;
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    public SnapshotManager(String tablePath, FileSystem fileSystem) {
        this.tablePath = tablePath;
        this.fileSystem = fileSystem;
        this.snapshotDir = tablePath + "/snapshot";
    }
    
    /**
     * Creates a new snapshot with the given manifest list
     */
    public Snapshot createSnapshot(String manifestList, long sequenceNumber, String commitUser) throws IOException {
        // Create snapshot directory if it doesn't exist
        Path snapshotPath = new Path(snapshotDir);
        if (!fileSystem.exists(snapshotPath)) {
            fileSystem.mkdirs(snapshotPath);
        }
        
        // Find the next available snapshot ID
        long nextSnapshotId = getNextSnapshotId();
        
        // Create snapshot filename
        String snapshotFileName = String.format("snapshot_%05d.json", nextSnapshotId);
        Path snapshotFilePath = new Path(snapshotDir, snapshotFileName);
        
        // Create snapshot object
        Snapshot snapshot = new Snapshot(
            nextSnapshotId,
            1, // schema ID
            manifestList,
            System.currentTimeMillis(),
            commitUser
        );
        
        // Write snapshot metadata to file using JSON
        String jsonContent = objectMapper.writeValueAsString(snapshot);
        try (FSDataOutputStream out = fileSystem.create(snapshotFilePath)) {
            out.write(jsonContent.getBytes("UTF-8"));
        }
        
        System.out.println("Created snapshot: " + snapshotFileName + " with ID: " + nextSnapshotId);
        
        return snapshot;
    }
    
    /**
     * Gets the latest snapshot
     */
    public Snapshot getLatestSnapshot() throws IOException {
        List<Snapshot> snapshots = getAllSnapshots();
        if (snapshots.isEmpty()) {
            return null;
        }
        // Return the snapshot with the highest ID (most recent)
        return snapshots.stream()
            .max(Comparator.comparingLong(Snapshot::getId))
            .orElse(null);
    }
    
    /**
     * Gets all snapshots sorted by ID
     */
    public List<Snapshot> getAllSnapshots() throws IOException {
        Path snapshotPath = new Path(snapshotDir);
        if (!fileSystem.exists(snapshotPath)) {
            return new ArrayList<>();
        }
        
        FileStatus[] files = fileSystem.listStatus(snapshotPath);
        if (files == null || files.length == 0) {
            return new ArrayList<>();
        }
        
        List<Snapshot> snapshots = new ArrayList<>();
        Pattern snapshotPattern = Pattern.compile("snapshot_(\\d+)\\.json");
        
        for (FileStatus file : files) {
            String fileName = file.getPath().getName();
            Matcher matcher = snapshotPattern.matcher(fileName);
            if (matcher.matches()) {
                try {
                    long snapshotId = Long.parseLong(matcher.group(1));
                    // Read the snapshot file
                    try (FSDataInputStream in = fileSystem.open(file.getPath())) {
                        byte[] buffer = new byte[(int)file.getLen()];
                        in.readFully(buffer);
                        String jsonContent = new String(buffer, "UTF-8");
                        
                        Snapshot snapshot = objectMapper.readValue(jsonContent, Snapshot.class);
                        snapshots.add(snapshot);
                    }
                } catch (IOException e) {
                    System.err.println("Error reading snapshot file " + fileName + ": " + e.getMessage());
                }
            }
        }
        
        // Sort snapshots by ID
        snapshots.sort(Comparator.comparingLong(Snapshot::getId));
        return snapshots;
    }
    
    /**
     * Gets snapshots that have IDs greater than the given ID
     */
    public List<Snapshot> getSnapshotsAfterId(long id) throws IOException {
        List<Snapshot> allSnapshots = getAllSnapshots();
        return allSnapshots.stream()
            .filter(snapshot -> snapshot.getId() > id)
            .collect(Collectors.toList());
    }
    
    /**
     * Finds the next available snapshot ID by looking at existing snapshots
     */
    private long getNextSnapshotId() throws IOException {
        List<Snapshot> snapshots = getAllSnapshots();
        if (snapshots.isEmpty()) {
            return 1; // Start with ID 1
        }
        
        // Find the highest existing ID and add 1
        return snapshots.stream()
            .mapToLong(Snapshot::getId)
            .max()
            .orElse(0) + 1;
    }
    
    public String getTablePath() {
        return tablePath;
    }
    
    public FileSystem getFileSystem() {
        return fileSystem;
    }
    
    public String getSnapshotDir() {
        return snapshotDir;
    }
}