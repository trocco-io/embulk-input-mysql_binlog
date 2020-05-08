package org.embulk.input.mysql_binlog.model;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.xml.bind.DatatypeConverter;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;


@Data
@NoArgsConstructor
public class Cell {
    private Object value;
    private Column column;
    // TODO: use embulk time formatter ?
    private SimpleDateFormat timeFormat =  new SimpleDateFormat("HH:mm:ss");
    private SimpleDateFormat dateFormat =  new SimpleDateFormat("yyyy-MM-dd");
    private SimpleDateFormat timestampFormat =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS XXX");

    public Cell(Object value, Column column){
        this.value = value;
        this.column = column;
    }

    // TODO: should be nullable?
    public String getValueWithString(){
        if (value == null) {
            return "null";
        }
        switch (column.getJdbcType()){
            case BIT:
            case BOOLEAN:
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
                return String.valueOf(value);
            case TIME:
                Timestamp time = new Timestamp((long) value);
                return timeFormat.format(time);
            case TIMESTAMP:
                Timestamp ts = new Timestamp((long) value);
                return timestampFormat.format(ts);
            case DATE:
                Date date = new Date((long) value);
                return dateFormat.format(date);
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
