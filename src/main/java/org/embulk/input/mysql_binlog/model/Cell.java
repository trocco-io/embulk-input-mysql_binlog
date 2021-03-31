package org.embulk.input.mysql_binlog.model;


import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.embulk.input.mysql_binlog.MysqlBinlogClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;


@Data
@NoArgsConstructor
public class Cell {
    private Object value;
    private Column column;
    private final Logger logger = LoggerFactory.getLogger(MysqlBinlogClient.class);

    private SimpleDateFormat timeFormat =  new SimpleDateFormat("HH:mm:ss");
    private SimpleDateFormat dateFormat =  new SimpleDateFormat("yyyy-MM-dd");
    private SimpleDateFormat timestampFormat =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS XXX");

    public Cell(Object value, Column column){
        this.value = value;
        this.column = column;
    }

    public String getValueWithString(){
        logger.debug(column.toString());
        if (value == null) {
            return null;
        }

        switch (column.getTypeName()) {
            case "BIT":
            case "BOOLEAN":
            case "INT":
            case "YEAR":
            case "TINYINT":
            case "SMALLINT":
            case "MEDIUMINT":
            case "BIGINT":
            case "REAL":
            case "NUMERIC":
            case "DECIMAL":
            case "CHAR":
            case "VARCHAR":
            case "DOUBLE":
            case "VARBINARY":
                return String.valueOf(value);
            case "FLOAT":
                double valDouble = Float.parseFloat(String.valueOf(value));
                return String.valueOf(valDouble);
            case "JSON":
                try {
                    return JsonBinary.parseAsString((byte[]) value);
                } catch (Exception e) {
                    return "";
                }
            case "TEXT":
            case "TINYTEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
            case "BLOB":
            case "TINYBLOB":
            case "MEDIUMBLOB":
            case "LONGBLOB":
                // TODO: check db encoding for blob
                // if not utf8 return hex
                // return "0x" + DatatypeConverter.printHexBinary((byte[]) value);
                return new String((byte[]) value);
            case "ENUM":
                int idx = Integer.parseInt(String.valueOf(value));
                String enumVal = column.getEnumValues().get(idx - 1);
                return enumVal.substring(1, enumVal.length() - 1);
            case "SET":
                StringBuilder sb = new StringBuilder();
                int index = 0;
                boolean first = true;
                long indexes = Long.parseLong(String.valueOf(value));
                int optionLen = column.getEnumValues().size();
                while (indexes != 0L) {
                    if (indexes % 2L != 0) {
                        if (first) {
                            first = false;
                        }
                        else {
                            sb.append(',');
                        }
                        if (index < optionLen) {
                            String setVal = column.getEnumValues().get(index);
                            sb.append(setVal, 1, setVal.length() - 1);
                        }
                        else {
                            logger.warn("Found unexpected index '{}' on column {}", index, column);
                        }
                    }
                    ++index;
                    indexes = indexes >>> 1;
                }
                return sb.toString();
            case "DATETIME":
            case "TIMESTAMP":
                Timestamp ts = new Timestamp((long) value);
                return timestampFormat.format(ts);
            case "DATE":
                Date date = new Date((long) value);
                return dateFormat.format(date);
            case "TIME":
                Timestamp time = new Timestamp((long) value);
                return timeFormat.format(time);
            default:
                logger.warn(column.toString());
                throw new RuntimeException("unknown data type " + this);
        }
    }
}
