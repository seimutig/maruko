package org.example.tablestore.format;

import java.util.Map;

public class Snapshot {
    private long id;
    private long schemaId;
    private String manifestList;
    private long commitTime;
    private String commitUser;
    private Map<String, String> properties;
    private long watermark;

    // Default constructor for Jackson deserialization
    public Snapshot() {
    }

    public Snapshot(long id, long schemaId, String manifestList, long commitTime, String commitUser) {
        this.id = id;
        this.schemaId = schemaId;
        this.manifestList = manifestList;
        this.commitTime = commitTime;
        this.commitUser = commitUser;
        this.properties = null; // Initialize as needed
        this.watermark = 0; // Initialize as needed
    }

    // Getters
    public long getId() {
        return id;
    }

    public long getSchemaId() {
        return schemaId;
    }

    public String getManifestList() {
        return manifestList;
    }

    public long getCommitTime() {
        return commitTime;
    }

    public String getCommitUser() {
        return commitUser;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public long getWatermark() {
        return watermark;
    }

    public void setWatermark(long watermark) {
        this.watermark = watermark;
    }
}