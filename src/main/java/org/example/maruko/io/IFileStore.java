package org.example.maruko.io;

import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface IFileStore {
    String writeData(List<Map<String, Object>> records, Map<String, String> partitionSpec, int bucket) throws IOException;
    String getTablePath();
    FileSystem getFileSystem();
}