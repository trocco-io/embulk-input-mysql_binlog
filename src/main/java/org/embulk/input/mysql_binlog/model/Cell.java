package org.embulk.input.mysql_binlog.model;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.xml.bind.DatatypeConverter;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class Cell {
    private Object value;
    private Column column;

    // TODO: should be nullable?
    public String getValueWithString(){
        if (value == null) {
            return "null";
        }
        switch (column.getJdbcType()){
            case BIT:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case REAL:
            case DOUBLE:
            case NUMERIC:
            case DECIMAL:
            case CHAR:
            case VARCHAR:
            case TIME:
            case TIMESTAMP:
            case DATE:
                return String.valueOf(value);
            case LONGVARCHAR:
            case LONGNVARCHAR:
                return new String((byte[]) value);
            case BLOB:
            case LONGVARBINARY:
                return "0x" + DatatypeConverter.printHexBinary((byte[]) value);
            default:
                throw new RuntimeException("unknown data type " + this);
        }
    }
}
