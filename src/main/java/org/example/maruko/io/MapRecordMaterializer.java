package org.example.maruko.io;

import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;

import java.util.Map;
import java.util.HashMap;

public class MapRecordMaterializer extends RecordMaterializer<Map<String, Object>> {
    private final MessageType schema;
    private MapConverter rootConverter;

    public MapRecordMaterializer(MessageType schema) {
        this.schema = schema;
        this.rootConverter = new MapConverter(schema);
    }

    @Override
    public Map<String, Object> getCurrentRecord() {
        return rootConverter.getCurrentRecord();
    }

    @Override
    public GroupConverter getRootConverter() {
        return rootConverter;
    }

    static class MapConverter extends GroupConverter {
        private final GroupType schema;
        private Map<String, Object> currentRecord;
        private PrimitiveConverter[] fieldConverters;

        public MapConverter(GroupType schema) {
            this.schema = schema;
            this.fieldConverters = new PrimitiveConverter[schema.getFieldCount()];

            // Initialize converters for each field
            for (int i = 0; i < schema.getFieldCount(); i++) {
                Type field = schema.getType(i);
                if (field.isPrimitive()) {
                    fieldConverters[i] = new FieldConverter(field.getName());
                }
            }
        }

        @Override
        public void start() {
            currentRecord = new HashMap<>();
        }

        @Override
        public void end() {
            // Record is complete
        }

        @Override
        public PrimitiveConverter getConverter(int fieldIndex) {
            return fieldConverters[fieldIndex];
        }

        public Map<String, Object> getCurrentRecord() {
            return currentRecord;
        }

        private class FieldConverter extends PrimitiveConverter {
            private String fieldName;

            public FieldConverter(String fieldName) {
                this.fieldName = fieldName;
            }

            @Override
            public void addBinary(org.apache.parquet.io.api.Binary value) {
                if (currentRecord != null) {
                    currentRecord.put(fieldName, value.toStringUsingUTF8());
                }
            }

            @Override
            public void addInt(int value) {
                if (currentRecord != null) {
                    currentRecord.put(fieldName, value);
                }
            }

            @Override
            public void addLong(long value) {
                if (currentRecord != null) {
                    currentRecord.put(fieldName, value);
                }
            }

            @Override
            public void addFloat(float value) {
                if (currentRecord != null) {
                    currentRecord.put(fieldName, value);
                }
            }

            @Override
            public void addDouble(double value) {
                if (currentRecord != null) {
                    currentRecord.put(fieldName, value);
                }
            }

            @Override
            public void addBoolean(boolean value) {
                if (currentRecord != null) {
                    currentRecord.put(fieldName, value);
                }
            }
        }
    }
}