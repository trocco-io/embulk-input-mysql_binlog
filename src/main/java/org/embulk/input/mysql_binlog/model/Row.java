package org.embulk.input.mysql_binlog.model;

import lombok.Data;
import java.util.List;

@Data
public class Row
{
    private List<Cell> cells;
}
