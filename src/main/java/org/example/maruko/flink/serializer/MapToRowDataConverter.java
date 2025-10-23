package org.example.maruko.flink.serializer;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.example.maruko.MarukoLogger;
import org.slf4j.Logger;

import java.math.BigDecimal;
import java.util.*;

/**
 * Converts Java Map to Flink RowData for TableStore integration.
 * Handles dynamic schemas from Flink SQL DDL.
 */
public class MapToRowDataConverter {
    
    private static final Logger logger = MarukoLogger.getLogger(MapToRowDataConverter.class);
    private final RowType rowType;
    
    public MapToRowDataConverter(RowType rowType) {
        this.rowType = rowType;
    }
    
    /**
     * Convert Map<String, Object> to RowData using the dynamic schema
     */
    public GenericRowData convert(Map<String, Object> recordMap) {
        if (recordMap == null) {
            logger.debug("MapToRowDataConverter: recordMap is null");
            return null;
        }
        
        if (rowType == null) {
            logger.debug("MapToRowDataConverter: rowType is null");
            return null;
        }
        
        logger.debug("MapToRowDataConverter: Converting record with {} fields, RowType has {} fields", recordMap.size(), rowType.getFields().size());
        logger.debug("MapToRowDataConverter: Record content: {}", recordMap);
        
        List<RowField> fields = rowType.getFields();
        Object[] values = new Object[fields.size()];
        
        for (int i = 0; i < fields.size(); i++) {
            RowField field = fields.get(i);
            String fieldName = field.getName();
            LogicalType fieldType = field.getType();
            
            Object value = recordMap.get(fieldName);
            logger.debug("MapToRowDataConverter: Processing field '{}', value: {}, type: {}", fieldName, value, fieldType);
            values[i] = convertValue(value, fieldType);
        }
        
        GenericRowData result = GenericRowData.of(values);
        logger.debug("MapToRowDataConverter: Successfully converted to RowData with arity {}", result.getArity());
        return result;
    }
    
    /**
     * Convert individual value based on logical type
     */
    private Object convertValue(Object value, LogicalType type) {
        if (value == null) {
            return null;
        }
        
        // Handle ByteBuffer specially (common when reading from Parquet)
        if (value instanceof java.nio.ByteBuffer) {
            java.nio.ByteBuffer buffer = (java.nio.ByteBuffer) value;
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            value = new String(bytes);
        }
        
        switch (type.getTypeRoot()) {
            case VARCHAR:
            case CHAR:
                return StringData.fromString(value.toString());
                
            case INTEGER:
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                } else {
                    try {
                        return Integer.parseInt(value.toString());
                    } catch (NumberFormatException e) {
                        return 0;
                    }
                }
                
            case BIGINT:
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                } else {
                    try {
                        return Long.parseLong(value.toString());
                    } catch (NumberFormatException e) {
                        return 0L;
                    }
                }
                
            case DOUBLE:
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                } else {
                    try {
                        return Double.parseDouble(value.toString());
                    } catch (NumberFormatException e) {
                        return 0.0;
                    }
                }
                
            case FLOAT:
                if (value instanceof Number) {
                    return ((Number) value).floatValue();
                } else {
                    try {
                        return Float.parseFloat(value.toString());
                    } catch (NumberFormatException e) {
                        return 0.0f;
                    }
                }
                
            case BOOLEAN:
                if (value instanceof Boolean) {
                    return (Boolean) value;
                } else {
                    return Boolean.parseBoolean(value.toString());
                }
                
            case SMALLINT:
                if (value instanceof Number) {
                    return ((Number) value).shortValue();
                } else {
                    try {
                        return Short.parseShort(value.toString());
                    } catch (NumberFormatException e) {
                        return (short) 0;
                    }
                }
                
            case TINYINT:
                if (value instanceof Number) {
                    return ((Number) value).byteValue();
                } else {
                    try {
                        return Byte.parseByte(value.toString());
                    } catch (NumberFormatException e) {
                        return (byte) 0;
                    }
                }
                
            case DECIMAL:
                if (value instanceof BigDecimal) {
                    DecimalType decimalType = (DecimalType) type;
                    return DecimalData.fromBigDecimal((BigDecimal) value, decimalType.getPrecision(), decimalType.getScale());
                } else {
                    try {
                        DecimalType decimalType = (DecimalType) type;
                        BigDecimal bd = new BigDecimal(value.toString());
                        return DecimalData.fromBigDecimal(bd, decimalType.getPrecision(), decimalType.getScale());
                    } catch (Exception e) {
                        return null;
                    }
                }
                
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // Handle timestamp as long value (milliseconds since epoch)
                if (value instanceof java.sql.Timestamp) {
                    java.sql.Timestamp ts = (java.sql.Timestamp) value;
                    return ts.getTime();  // Returns long milliseconds
                } else if (value instanceof java.time.LocalDateTime) {
                    java.time.LocalDateTime ldt = (java.time.LocalDateTime) value;
                    return java.sql.Timestamp.valueOf(ldt).getTime();
                } else if (value instanceof Long) {
                    return value;  // Already in long form
                } else if (value instanceof Number) {
                    return ((Number) value).longValue();
                } else {
                    // Try to parse as string
                    try {
                        // If it's a timestamp string, parse it to long
                        java.sql.Timestamp ts = java.sql.Timestamp.valueOf(value.toString());
                        return ts.getTime();
                    } catch (Exception e) {
                        logger.debug("MapToRowDataConverter: Could not parse timestamp value '{}', defaulting to current time", value);
                        return System.currentTimeMillis();
                    }
                }
                
            default:
                // For unsupported types, convert to string
                return StringData.fromString(value.toString());
        }
    }
}