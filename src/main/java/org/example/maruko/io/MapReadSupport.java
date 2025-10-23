package org.example.maruko.io;

import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.hadoop.conf.Configuration;
import java.util.Map;

public class MapReadSupport extends ReadSupport<Map<String, Object>> {

    @Override
    public ReadContext init(
            Configuration configuration,
            Map<String, String> keyValueMetaData,
            MessageType fileSchema) {
        
        return new ReadContext(fileSchema, keyValueMetaData);
    }

    @Override
    public RecordMaterializer<Map<String, Object>> prepareForRead(
            Configuration configuration,
            Map<String, String> keyValueMetaData,
            MessageType fileSchema,
            ReadContext readContext) {
        
        return new MapRecordMaterializer(fileSchema);
    }
}