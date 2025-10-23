package org.example.maruko.io;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type.Repetition;
import java.util.Map;
import java.util.HashMap;

public class MapParquetWriter extends WriteSupport<Map<String, Object>> {
    private MessageType schema;
    private RecordConsumer recordConsumer;

    @Override
    public WriteContext init(org.apache.hadoop.conf.Configuration configuration) {
        // Schema should be set externally
        return new WriteContext(schema, new HashMap<>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(Map<String, Object> record) {
        recordConsumer.startMessage();
        
        for (Type field : schema.getFields()) {
            String fieldName = field.getName();
            Object value = record.get(fieldName);
            
            if (value != null) {
                recordConsumer.startField(fieldName, 0);
                
                // Write value based on the field's primitive type
                if (field.isPrimitive()) {
                    PrimitiveType.PrimitiveTypeName primitiveType = 
                        field.asPrimitiveType().getPrimitiveTypeName();
                    
                    switch (primitiveType) {
                        case BINARY:
                            recordConsumer.addBinary(
                                org.apache.parquet.io.api.Binary.fromString(value.toString()));
                            break;
                        case INT64:
                            recordConsumer.addLong(((Number) value).longValue());
                            break;
                        case INT32:
                            recordConsumer.addInteger(((Number) value).intValue());
                            break;
                        case DOUBLE:
                            recordConsumer.addDouble(((Number) value).doubleValue());
                            break;
                        case FLOAT:
                            recordConsumer.addFloat(((Number) value).floatValue());
                            break;
                        case BOOLEAN:
                            recordConsumer.addBoolean((Boolean) value);
                            break;
                        case INT96: // For timestamps
                            if (value instanceof java.sql.Timestamp) {
                                recordConsumer.addBinary(
                                    org.apache.parquet.io.api.Binary.fromReusedByteArray(
                                        ((java.sql.Timestamp) value).toString().getBytes()));
                            } else {
                                recordConsumer.addBinary(
                                    org.apache.parquet.io.api.Binary.fromString(value.toString()));
                            }
                            break;
                        default:
                            recordConsumer.addBinary(
                                org.apache.parquet.io.api.Binary.fromString(value.toString()));
                    }
                }
                
                recordConsumer.endField(fieldName, 0);
            }
        }
        
        recordConsumer.endMessage();
    }

    public void setSchema(MessageType schema) {
        this.schema = schema;
    }
    
    @Override
    public String getName() {
        return "MapParquetWriter";
    }
}