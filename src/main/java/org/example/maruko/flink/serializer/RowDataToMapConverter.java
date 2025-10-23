package org.example.maruko.flink.serializer;

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
            System.out.println("RowDataToMapConverter: rowData is null");
            return null;
        }
        
        if (rowType == null) {
            System.out.println("RowDataToMapConverter: rowType is null");
            return new HashMap<>();
        }
        
        List<RowType.RowField> fields = rowType.getFields();
        if (fields.isEmpty()) {
            System.out.println("RowDataToMapConverter: rowType has no fields");
            return new HashMap<>();
        }
        
        Map<String, Object> result = new HashMap<>();
        
        for (int i = 0; i < fields.size(); i++) {
            RowType.RowField field = fields.get(i);
            String fieldName = field.getName();
            LogicalType fieldType = field.getType();
            
            try {
                Object value = convertField(rowData, i, fieldType);
                result.put(fieldName, value);  // Put the value regardless if it's null to preserve the structure
                
                // Debug: Print if field is null
                if (value == null && !isFieldNull(rowData, i, fieldType)) {
                    System.out.println("RowDataToMapConverter: Field '" + fieldName + "' is null but rowData at index " + i + " is not null");
                }
            } catch (Exception e) {
                System.err.println("RowDataToMapConverter: Failed to convert field " + fieldName + ": " + e.getMessage());
                e.printStackTrace();
                result.put(fieldName, null);  // Add null value to preserve structure
                // Continue with other fields
            }
        }
        
        // Debug: Print the result to see what we got
        if (result.size() > 0 && result.values().stream().allMatch(v -> v == null)) {
            System.out.println("RowDataToMapConverter: All values in result are null, RowType fields: " + rowType.getFieldNames());
            System.out.println("RowData: Row arity = " + rowData.getArity());
        }
        
        return result;
    }
    
    /**
     * Check if a field is null in a safe way
     */
    private boolean isFieldNull(RowData rowData, int index, LogicalType type) {
        try {
            if (index < 0 || index >= rowData.getArity()) {
                return true;
            }
            
            switch (type.getTypeRoot()) {
                case VARCHAR:
                case CHAR:
                    return rowData.isNullAt(index);
                case INTEGER:
                case BIGINT:
                case DOUBLE:
                case BOOLEAN:
                case SMALLINT:
                case TINYINT:
                    return rowData.isNullAt(index);
                case FLOAT:
                    return rowData.isNullAt(index);
                case DECIMAL:
                    return rowData.isNullAt(index);
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_TIME_ZONE:
                    return rowData.isNullAt(index);
                case DATE:
                    return rowData.isNullAt(index);
                case TIME_WITHOUT_TIME_ZONE:
                    return rowData.isNullAt(index);
                default:
                    return rowData.isNullAt(index);
            }
        } catch (Exception e) {
            return true;
        }
    }
    
    /**
     * Convert individual field based on its logical type
     */
    private Object convertField(RowData rowData, int index, LogicalType type) {
        // Check bounds
        if (index < 0 || index >= rowData.getArity()) {
            System.out.println("RowDataToMapConverter: Index " + index + " is out of bounds for RowData arity " + rowData.getArity());
            return null;
        }
        
        if (rowData.isNullAt(index)) {
            return null;
        }
        
        switch (type.getTypeRoot()) {
            case VARCHAR:
            case CHAR:
                return convertStringField(rowData, index);
                
            case INTEGER:
                try {
                    return rowData.getInt(index);
                } catch (Exception e) {
                    System.out.println("RowDataToMapConverter: Could not get INTEGER at index " + index + ", exception: " + e.getMessage());
                    return null;
                }
                
            case BIGINT:
                try {
                    return rowData.getLong(index);
                } catch (Exception e) {
                    System.out.println("RowDataToMapConverter: Could not get BIGINT at index " + index + ", exception: " + e.getMessage());
                    return null;
                }
                
            case DOUBLE:
                try {
                    return rowData.getDouble(index);
                } catch (Exception e) {
                    System.out.println("RowDataToMapConverter: Could not get DOUBLE at index " + index + ", exception: " + e.getMessage());
                    return null;
                }
                
            case FLOAT:
                try {
                    return (double) rowData.getFloat(index);  // Convert to double for consistency
                } catch (Exception e) {
                    System.out.println("RowDataToMapConverter: Could not get FLOAT at index " + index + ", exception: " + e.getMessage());
                    return null;
                }
                
            case BOOLEAN:
                try {
                    return rowData.getBoolean(index);
                } catch (Exception e) {
                    System.out.println("RowDataToMapConverter: Could not get BOOLEAN at index " + index + ", exception: " + e.getMessage());
                    return null;
                }
                
            case SMALLINT:
                try {
                    return (int) rowData.getShort(index);  // Convert to int for consistency
                } catch (Exception e) {
                    System.out.println("RowDataToMapConverter: Could not get SMALLINT at index " + index + ", exception: " + e.getMessage());
                    return null;
                }
                
            case TINYINT:
                try {
                    return (int) rowData.getByte(index);  // Convert to int for consistency
                } catch (Exception e) {
                    System.out.println("RowDataToMapConverter: Could not get TINYINT at index " + index + ", exception: " + e.getMessage());
                    return null;
                }
                
            case DECIMAL:
                try {
                    DecimalType decimalType = (DecimalType) type;
                    return rowData.getDecimal(index, decimalType.getPrecision(), decimalType.getScale());
                } catch (Exception e) {
                    System.out.println("RowDataToMapConverter: Could not get DECIMAL at index " + index + ", exception: " + e.getMessage());
                    return null;
                }
                
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                try {
                    TimestampType timestampType = (TimestampType) type;
                    return rowData.getTimestamp(index, timestampType.getPrecision());
                } catch (Exception e) {
                    System.out.println("RowDataToMapConverter: Could not get TIMESTAMP at index " + index + ", exception: " + e.getMessage());
                    return null;
                }
                
            case DATE:
                try {
                    return rowData.getInt(index); // Flink represents DATE as int (days since epoch)
                } catch (Exception e) {
                    System.out.println("RowDataToMapConverter: Could not get DATE at index " + index + ", exception: " + e.getMessage());
                    return null;
                }
                
            case TIME_WITHOUT_TIME_ZONE:
                try {
                    return rowData.getInt(index); // Flink represents TIME as int (milliseconds since midnight)
                } catch (Exception e) {
                    System.out.println("RowDataToMapConverter: Could not get TIME at index " + index + ", exception: " + e.getMessage());
                    return null;
                }
                
            default:
                // For other unsupported types, try to get as string representation
                try {
                    return convertStringField(rowData, index);
                } catch (Exception e) {
                    System.out.println("RowDataToMapConverter: Could not get field at index " + index + " with type " + type.getTypeRoot() + ", exception: " + e.getMessage());
                    return null;
                }
        }
    }
    
    /**
     * Convert string field (VARCHAR, CHAR)
     */
    private String convertStringField(RowData rowData, int index) {
        try {
            org.apache.flink.table.data.StringData stringData = rowData.getString(index);
            if (stringData == null) {
                return null;
            }
            return stringData.toString();
        } catch (Exception e) {
            System.out.println("RowDataToMapConverter: Could not get STRING at index " + index + ", exception: " + e.getMessage());
            return null;
        }
    }
}