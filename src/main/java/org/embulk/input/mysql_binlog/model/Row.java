package org.embulk.input.mysql_binlog.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.util.List;

@Data
@AllArgsConstructor
public class Row
{
    private List<Cell> cells;
    private Table table;

    public String toJsonString(){
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = this.mapper.createObjectNode();

        // TODO: convert all types, json, timestamp etc...
        for(Cell cell : cells){
            root.put(cell.getColumn().getName(), cell.getValueWithString());
        }
        return mapper.writeValueAsString(root);
    }
}
