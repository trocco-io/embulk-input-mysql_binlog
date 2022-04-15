package org.embulk.input.mysql_binlog.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
        ObjectNode root = mapper.createObjectNode();

        // TODO: convert all types, json, timestamp etc...
        for(Cell cell : cells){
            root.put(cell.getColumn().getName(), cell.getValueWithString());
        }
        // TODO: handle correclty
        try {
            return mapper.writeValueAsString(root);
        }catch (JsonProcessingException e){
            return "";
        }
    }
}
