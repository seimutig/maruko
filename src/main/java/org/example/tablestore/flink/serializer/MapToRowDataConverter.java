package org.example.tablestore.flink.serializer;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.RowType.RowField;

import java.math.BigDecimal;
import java.util.*;

/**
 * Converts Java Map to Flink RowData for TableStore integration.
 * Handles dynamic schemas from Flink SQL DDL.
 */
public class MapToRowDataConverter {
    
    private final RowType rowType;
    
    public MapToRowDataConverter(RowType rowType) {
        this.rowType = rowType;
    }
    
    /**
     * Convert Map<String, Object> to RowData using the dynamic schema
     */
    public GenericRowData convert(Map<String, Object> recordMap) {
        if (recordMap == null) {
            return null;
        }
        
        List<RowField> fields = rowType.getFields();
        Object[] values = new Object[fields.size()];
        
        for (int i = 0; i < fields.size(); i++) {
            RowField field = fields.get(i);
            String fieldName = field.getName();
            LogicalType fieldType = field.getType();
            
            Object value = recordMap.get(fieldName);
            values[i] = convertValue(value, fieldType);
        }
        
        return GenericRowData.of(values);
    }
    
    /**
     * Convert individual value based on logical type
     */
    private Object convertValue(Object value, LogicalType type) {
        if (value == null) {
            return null;
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
                
            default:
                // For unsupported types, convert to string
                return StringData.fromString(value.toString());
        }
    }
}