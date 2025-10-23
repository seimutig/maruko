package org.example.maruko.format;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.util.*;

public class ManifestManager {
    private String manifestDir;
    private FileSystem fs;
    private ObjectMapper objectMapper;

    public ManifestManager(String tablePath, FileSystem fs) {
        this.fs = fs;
        this.manifestDir = tablePath + "/manifest";
        this.objectMapper = new ObjectMapper();
    }

    public String writeManifestFile(List<ManifestEntry> entries) throws IOException {
        String fileName = "manifest_" + System.currentTimeMillis() + "_" + 
                         String.format("%05d", new Random().nextInt(10000)) + ".json";
        Path manifestPath = new Path(manifestDir, fileName);
        
        // Ensure manifest directory exists
        Path manifestDirPath = new Path(manifestDir);
        if (!fs.exists(manifestDirPath)) {
            fs.mkdirs(manifestDirPath);
        }
        
        try (FSDataOutputStream out = fs.create(manifestPath)) {
            String entriesJson = objectMapper.writeValueAsString(entries);
            out.writeUTF(entriesJson);
        }
        
        return fileName;
    }

    public List<ManifestEntry> readManifestFile(String fileName) throws IOException {
        Path manifestPath = new Path(manifestDir, fileName);
        
        try (FSDataInputStream in = fs.open(manifestPath)) {
            String entriesJson = in.readUTF();
            ManifestEntry[] entries = objectMapper.readValue(entriesJson, ManifestEntry[].class);
            return Arrays.asList(entries);
        }
    }

    public List<String> getAllManifestFiles() throws IOException {
        List<String> manifestFiles = new ArrayList<>();
        Path manifestDirPath = new Path(manifestDir);
        
        if (!fs.exists(manifestDirPath)) {
            return manifestFiles;
        }
        
        FileStatus[] files = fs.listStatus(manifestDirPath);
        for (FileStatus file : files) {
            String fileName = file.getPath().getName();
            if (fileName.startsWith("manifest_") && fileName.endsWith(".json")) {
                manifestFiles.add(fileName);
            }
        }
        
        return manifestFiles;
    }

    public void updateManifestEntry(ManifestEntry entry, String fileName) throws IOException {
        // Read existing manifest
        List<ManifestEntry> entries = readManifestFile(fileName);
        
        // Update the specific entry (for simplicity, we'll just add if not exists)
        boolean found = false;
        for (int i = 0; i < entries.size(); i++) {
            ManifestEntry existing = entries.get(i);
            if (existing.getFilePath().equals(entry.getFilePath()) && 
                existing.getBucket() == entry.getBucket() &&
                existing.getPartition().equals(entry.getPartition())) {
                entries.set(i, entry);
                found = true;
                break;
            }
        }
        
        if (!found) {
            entries.add(entry);
        }
        
        // Write back to file
        Path manifestPath = new Path(manifestDir, fileName);
        
        try (FSDataOutputStream out = fs.create(manifestPath, true)) { // overwrite
            String entriesJson = objectMapper.writeValueAsString(entries);
            out.writeUTF(entriesJson);
        }
    }
}