package org.example.tablestore.flink.serializer;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.data.StringData;

import java.util.*;

/**
 * Converts Flink RowData to Java Map for TableStore integration.
 * Handles dynamic schemas from Flink SQL DDL.
 */
public class RowDataToMapConverter {
    
    private final RowType rowType;
    
    public RowDataToMapConverter(RowType rowType) {
        this.rowType = rowType;
    }
    
    /**
     * Convert RowData to Map<String, Object> using the dynamic schema
     */
    public Map<String, Object> convert(RowData rowData) {
        if (rowData == null) {
            return null;
        }
        
        Map<String, Object> result = new HashMap<>();
        List<RowType.RowField> fields = rowType.getFields();
        
        for (int i = 0; i < fields.size(); i++) {
            RowType.RowField field = fields.get(i);
            String fieldName = field.getName();
            LogicalType fieldType = field.getType();
            
            try {
                Object value = convertField(rowData, i, fieldType);
                if (value != null) {
                    result.put(fieldName, value);
                }
            } catch (Exception e) {
                System.err.println("Warning: Failed to convert field " + fieldName + ": " + e.getMessage());
                // Continue with other fields
            }
        }
        
        return result;
    }
    
    /**
     * Convert individual field based on its logical type
     */
    private Object convertField(RowData rowData, int index, LogicalType type) {
        if (rowData.isNullAt(index)) {
            return null;
        }
        
        switch (type.getTypeRoot()) {
            case VARCHAR:
            case CHAR:
                return convertStringField(rowData, index);
                
            case INTEGER:
                return rowData.getInt(index);
                
            case BIGINT:
                return rowData.getLong(index);
                
            case DOUBLE:
                return rowData.getDouble(index);
                
            case FLOAT:
                return (double) rowData.getFloat(index); // Convert to double for consistency
                
            case BOOLEAN:
                return rowData.getBoolean(index);
                
            case SMALLINT:
                return (int) rowData.getShort(index); // Convert to int for consistency
                
            case TINYINT:
                return (int) rowData.getByte(index); // Convert to int for consistency
                
            case DECIMAL:
                return rowData.getDecimal(index, ((DecimalType) type).getPrecision(), ((DecimalType) type).getScale());
                
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                return rowData.getTimestamp(index, ((TimestampType) type).getPrecision());
                
            case DATE:
                return rowData.getInt(index); // Flink represents DATE as int (days since epoch)
                
            case TIME_WITHOUT_TIME_ZONE:
                return rowData.getInt(index); // Flink represents TIME as int (milliseconds since midnight)
                
            default:
                // For unsupported types, try to get as string representation
                return "Unsupported_Type_" + type.getTypeRoot();
        }
    }
    
    /**
     * Convert string field (VARCHAR, CHAR)
     */
    private String convertStringField(RowData rowData, int index) {
        StringData stringData = rowData.getString(index);
        if (stringData == null) {
            return null;
        }
        return stringData.toString();
    }
}